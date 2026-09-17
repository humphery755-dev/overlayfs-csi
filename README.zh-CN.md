# overlayfs-csi

[English](README.md) | 简体中文

本仓库实现了一个 Kubernetes [Container Storage Interface](https://github.com/container-storage-interface/spec/blob/master/spec.md) 驱动，以 [overlay 挂载](https://en.wikipedia.org/wiki/OverlayFS)的方式在共享的 _base_（基础层）之上提供持久卷。仅新增和修改的文件写入卷自己的数据目录；与 base 共有的内容以只读方式共享。

这在构建流水线中尤其有用：只要工具链支持增量编译，就无需每次运行都全量拷贝文件。

它的效果类似于支持高效[卷克隆](https://kubernetes.io/docs/concepts/storage/volume-pvc-datasource/)的 CSI 中的 `dataSource`，区别在于 base 卷的维护由本 CSI 自动完成。

本仓库同时提供了一个用 Rust 构建 Kubernetes CSI 的完整示例。

## 使用方法

- 卷以独立 [PVC](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) 的形式申请（使用本驱动的 StorageClass），并通过 `persistentVolumeClaim` 引用挂载（完整示例见 [`pod.yaml`](pod.yaml)）：

  ```yaml
  apiVersion: v1
  kind: PersistentVolumeClaim
  metadata:
    name: demo
  spec:
    accessModes: ["ReadWriteOnce"]
    storageClassName: overlayfs.csi.k8s.io
    resources:
      requests:
        storage: 10Gi
  ---
  # 在 pod spec 中：
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: demo
  ```

- StorageClass 使用 `volumeBindingMode: WaitForFirstConsumer`：卷只在使用该 PVC 的 Pod 被调度之后才供给。调度器把选中的节点写进 PVC 的 `volume.kubernetes.io/selected-node` 注解，该节点上的驱动实例随后创建预绑定的 PV `overlayfs-<pvc-uid>`（`nodeAffinity` 把卷钉在该节点）。因此 PVC 的数据落在该节点的本地磁盘上，之后挂载同一 PVC 的所有 Pod 都会被调度回同一节点。单节点集群无需任何操作；多节点集群直接让调度器选节点即可（除非确需手动固定，否则不要设置 `nodeName`）。

- PVC 中的数据是持久的：删除并重建 Pod 后数据仍在。关于 [generic ephemeral volume](https://kubernetes.io/docs/concepts/storage/projected-volumes/#generic-ephemeral-volume) 的注意事项见下文。

> [!WARNING]
> 不要用 generic ephemeral volume（pod spec 中的 `volumeClaimTemplate`）承载需要保留的数据。这类卷背后的 PVC 会随 Pod 一起删除，数据在 Pod 删除时即被销毁——不满足持久化需求。请使用上文所示的独立 PVC。

### Base 与 `.as_base`

- 只要存在有效的 base，新 stage 的卷就是叠加在其上的 overlay 文件系统；否则从空开始。

- PVC 数据本身的持久性与 base 无关——这是结构保证的。base 只是供_后续新_ PVC 使用的共享只读起点。

- 在卷内写入一个 `.as_base` 文件，Pod 即可声明该卷随后可被用作 base。当卷被 unstage（最后一个 Pod 卸载它）且当时不存在有效 base 时，当前环境——base 与卷增量的合并视图——会被固化为一个新 base，之后新建的 PVC 会叠加在它之上。若当时已存在有效 base，则跳过固化。

  - TODO: 未来可以通过检查 Pod 的退出状态来替代该标记。

- base 携带创建时间戳，超过 `--max-age-s`（默认 2592000 秒，即 30 天）后过期。过期 base 会在后台清理，除非仍有 overlay 挂载引用它们。当因此没有任何可用 base 时，下一个卷就从零开始（直到又有卷被固化为 base）。

- TTL 可以按卷覆盖：给 PVC 加注解 `overlayfs.csi.k8s.io/max-age-s: "<秒>"`，该卷固化出的 base 使用该值而非全局值。注解缺失或非法时回退全局 `maxAgeSeconds`。

### 底层存储

仅支持节点本地存储：base 与每个 PVC 的数据目录都位于 hostPath 存储根（Helm 值 `storageRoot`，默认 `/var/lib/overlayfs-csi`）之下——这也让「卷转 base」可以快速完成。每个节点维护自己的 base，PVC 的数据固定在其供给时所在的节点（见上文）。base 就是该路径下的普通目录，因此驱动 Pod 的重启与重新部署不会丢失它们。

支持任意卷类型并不困难：对支持高效[卷克隆](https://kubernetes.io/docs/concepts/storage/volume-pvc-datasource/)的 CSI，可以用克隆替代 overlay。

## 安装

> [!CAUTION]
> 目前完全处于实验状态，请自行承担风险。

1. 编译二进制并构建 docker 镜像：

   ```
   $ cd docker
   $ cross build -r --target-dir ../target-cross
   $ cp ../target-cross/release/overlayfs-csi .
   $ docker build -t overlayfs-csi .
   ```

   构建过程需要下载 `csi.proto` 和一个 protobuf 编译器。使用 `cross` 时，[`Cross.toml`](Cross.toml) 指向 [`docker/Dockerfile.cross`](docker/Dockerfile.cross)——一个预装 protoc 25.1 的 builder 镜像（离线场景友好：先把 protoc 内容放到 `docker/protoc-usr/` 下）。若在主机上直接 `cargo build`，需要自行指定 protoc：

   ```
   $ export PROTOC=/usr/local/bin/protoc PROTOC_INCLUDE=/usr/include   # 系统 protoc
   # 或复用仓库内置的：
   $ export PROTOC=$PWD/docker/protoc-usr/bin/protoc PROTOC_INCLUDE=$PWD/docker/protoc-usr/include
   ```

2. 按需定制 [Helm chart](https://helm.sh/) 的值（`chart/values.yaml`）：驱动名、`storageClassName`（默认取驱动名）、`storageRoot`、`maxAgeSeconds`。
3. 安装 chart
   ```
   $ helm install overlayfs-csi chart
   ```

部署完成后可按如下方式验证——[`pod.yaml`](pod.yaml) 会创建 PVC 并启动一个挂载到 `/test` 的 Pod：

1. 按需创建 namespace（幂等写法），然后应用示例
   ```
   $ kubectl create namespace overlayfs-csi --dry-run=client -o yaml | kubectl apply -f -
   $ kubectl apply -f pod.yaml
   ```
2. 写入数据并标记为 base 素材
   ```
   $ touch /test/hello
   $ touch /test/.as_base
   ```
3. 删除 Pod 后再次 `kubectl apply -f pod.yaml`：Pod 重新挂载同一个 PVC，数据仍在。
   ```
   $ ls /test
   hello
   $ touch /test/hi
   ```
   写入只落在卷自己的数据目录；base（若存在）永不修改，也不会拷贝未触及的文件。
4. 第 2 步的 Pod 删除时，其 `.as_base` 标记使卷在 unstage 时被固化为新 base（前提是当时没有有效 base）。此后创建的_新_ PVC 会叠加在它之上，因此开箱即含 `hello`。

### 长期停机（数天到数月）

同样的示例可以经受任意长的停机——被删除的只有 Pod，PVC（及其在节点上的数据目录）始终保留：

```
$ kubectl delete pod test        # 只删 Pod —— 切勿 kubectl delete -f pod.yaml，那会连 PVC 一起删除
$ kubectl apply -f pod.yaml      # 数天后：同名 PVC 复用，所有变更原样回来
```

`maxAgeSeconds` 决定停机后_完整_环境（base + 你的变更）能回粘多久；PVC 数据本身不依赖它。把它设置为大于预期停机间隔即可（见 [`chart/values.yaml`](chart/values.yaml)）。

## 实现细节

- 单个 Rust 二进制实现所需的 Identity、Node 与 Controller CSI 服务。kubelet 通过 UNIX socket 与 Node 服务通信。
- 以 DaemonSet 在每个节点运行一个实例，遵循 Kubernetes CSI 设计。`CSIDriver` 对象只声明 `Persistent` 卷生命周期模式（不支持 inline ephemeral 卷），Node 能力通告 `STAGE_UNSTAGE_VOLUME`。
- 每个实例拥有一个 hostPath 存储根（`--bases` 参数，即 `storageRoot` 值），其下三个子目录：`bases/`（共享只读 base 树）、`volumes/<pv-name>/`（每 PVC 数据目录）、`work/<pv-name>/`（overlay work 目录）。
- 供给不经过 CSI Controller 服务，而是由每个节点实例内的控制循环 watch 全部 PVC 完成：
  - 满足以下条件的 PVC（处于 `Pending`、`storageClassName` 与本驱动的 StorageClass 匹配、`volume.kubernetes.io/selected-node` 注解等于本节点）会得到预绑定 PV `overlayfs-<pvc-uid>`（带 `app.kubernetes.io/managed-by: overlayfs-csi` 标签、回收策略 `Delete`、nodeAffinity 锁定本节点）。该操作幂等，并有 30 秒的 reconcile 循环在重启后重放。
  - PVC 被删除会触发删除对应 PV，并移除本节点上相应的数据目录与 work 目录。
- 挂载按通告的 `STAGE_UNSTAGE_VOLUME` 能力走两段式：
  - `NodeStageVolume` 把卷数据目录（作为 upper 层，配一个 `work/` 目录）overlay 挂载到有效 base 之上、挂到 staging 路径；没有有效 base 时则直接 bind mount 数据目录。
  - `NodePublishVolume` 把 staging 路径 bind mount 进 Pod；`NodeUnpublishVolume`/`NodeUnstageVolume` 按相反顺序（幂等地）卸载。
- `NodeUnstageVolume` 承担前述 base 固化：若卷内存在 `.as_base` 且无有效 base，当前视图会被拷入 `bases/<uuid>/` 并打上新鲜时间戳（先用 `cp --reflink=auto`，失败时回滚目标目录）。合并视图包含卷 stage 时所用的 base——在卸载 staging 挂载之前从 `/proc/self/mountinfo` 读取，因此即使它的 TTL 已过期，其内容仍会并入固化的树。stage 决策只评估一次并被固化复用，以避免并发 unstage 把无关卷的数据合并进被固化的 base。
- 每个节点的后台 janitor（30 秒间隔）承担：
  - base TTL 清理：时间戳早于 `--max-age-s` 的 base 会被移除，除非 `/proc/self/mountinfo` 显示仍有 overlay 挂载的 `lowerdir` 指向它们（通过解析 mountinfo 做引用检测）。
  - 孤儿 GC：没有对应 managed PV 的 `volumes/` 目录——且超过 5 分钟宽限期——会被移除；`work/` 目录在其对应卷目录不存在时同样被移除。
- 若某次 `/proc/self/mountinfo` 快照解析出零个 overlay 挂载，janitor 会记录告警并跳过整轮清理——因为此时的引用检查无法保证不会静默放行。

## TODO

- 通过 Pod 状态（而非 `.as_base` 标记）推断卷能否用作 base（见上文）。
- 支持其他底层存储（见上文）。
- 通过标签区分不同类别的 base。
