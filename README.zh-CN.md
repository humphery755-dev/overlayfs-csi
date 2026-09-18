# overlayfs-csi

[English](README.md) | 简体中文

一个 Kubernetes [CSI](https://github.com/container-storage-interface/spec/blob/master/spec.md) 驱动，具备两大核心能力：

1. **一个注解让 Pod 变成一台小型虚拟机。** 容器内安装或修改的一切——`apt install`、改配置、整个系统目录树——以写穿透语义持久化，Pod 删除重建后原样保留。无需 PVC。
2. **以 overlayfs 增量方式提供持久卷。** 卷只保存新增和修改的文件，叠在共享只读 _base_ 树之上——克隆式的环境（构建流水线、预装工具链）几乎零成本。

两者由同一个注解名驱动：`overlayfs.csi.k8s.io/max-age-s`——打在 **Pod** 上开启 VM 模式；打在 **PVC** 上覆盖 base 固化 TTL。

| 特性 | 你写什么 | 你得到什么 |
|---|---|---|
| **VM 模式** | 一个 Pod 注解 | 系统级修改跨 Pod 重建保留——写穿透，没有「退出保存」步骤 |
| **持久卷** | 普通 [PVC](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) | 数据随 Pod 删除重建保留；只有增量落盘 |
| **Base 共享** | `touch <卷根>/.as_base` | 环境固化为共享只读 base，供后续 PVC 叠加 |
| **TTL 注解** | 同一个注解名，两级生效 | Pod → 快照保留期；PVC → 固化 base 的 TTL |

本仓库同时提供了一个用 Rust 构建 Kubernetes CSI 的完整示例。

## VM 模式：像虚拟机一样的 Pod

给 Pod 打上 `overlayfs.csi.k8s.io/max-age-s: "<秒>"` 注解：

```yaml
metadata:
  name: vm-demo
  annotations:
    overlayfs.csi.k8s.io/max-age-s: "7776000"   # 删除后快照保留 90 天
spec:
  containers:
    - name: vm-demo
      image: debian:bullseye-slim
      command: ["sleep", "infinity"]   # 必须显式指定 command
```

然后像用一台机器一样用它：

```
$ kubectl apply -f examples/dev-pod.yaml   # 创建（webhook 自动注入一切）
$ kubectl exec -it vm-demo -- bash         # apt install、改 /etc、编译——随便
$ kubectl delete pod vm-demo               # 删除即时完成：不存在「做快照」的动作
$ kubectl apply -f examples/dev-pod.yaml   # 任意久后重建：一切原样回来
```

工作原理——chart 部署的 mutating webhook 在 Pod 创建时改写带注解的 Pod：

- 上述每个系统目录（`/etc`、`/usr`、`/var`、`/opt`、`/root`、`/home`、`/srv`）都会叠一层 overlay：`upperdir` 位于 hostPath 快照目录（`<storageRoot>/vm/<namespace>/<pod-name>`），镜像内容经 bind mount 固定为 lower 层。每次写入直接落到快照目录的磁盘上——**没有「退出时保存」步骤**，SIGKILL/OOM/断电都不会丢已确认的写入；删除 Pod 不触发任何数据动作。
- 重建时重新挂同一批 upperdir：修改全部回来。删除过的文件仍然是删除状态（whiteout 持久）。换了更新的镜像重建，你的修改依旧叠加在新内容之上。
- 注解值兼任保留 TTL：Pod 消失后，controller 的 GC 在这么多秒之后删除快照目录。到期之前，重建的成本只是重新 apply 一次。
- webhook 还会注入 `privileged: true`（mount 的内核要求），并把每个容器的 command 包上一层 init 脚本——脚本完成挂载后 re-exec 你的原命令。

硬性规则（刻意 fail-fast——静默不持久等于静默丢数据）：

- 注解值非法、缺少 `command`、`runAsNonRoot`/非 root uid 的 Pod 会在创建时**被拒绝**，并给出原因。
- webhook 短暂不可用时（其 `failurePolicy` 为 `Ignore`，普通 Pod 永不受阻），controller 检测到「带注解但未注入」的 VM Pod 会将其删除，重建时即完成注入。
- 系统目录内出现会被遮蔽的异常挂载点时，init 脚本直接让容器启动失败，绝不静默隐藏。

已知限制：仅普通容器被持久化（initContainer/sidecar 不含）；镜像须含 POSIX `sh`；`/tmp`、`/run` 不持久而 `machine-id`、ssh host keys 持久（VM 语义）；快照在节点本地盘上，多节点集群需要 Pod 重新调度回同一节点。另有两点 overlayfs 固有行为需要了解：跨层目录 `rename` 会返回 `EXDEV`（`mv`、`git` 等规范工具会退化为「拷贝+删除」；特别大的目录会多一次拷贝）；对镜像内已有文件的_首次_写入会触发该文件的完整 copy-up（之后写入无额外开销）。

面向「每人一个开发环境」的现成模板见 [`examples/dev-pod.yaml`](examples/dev-pod.yaml)（改 name/image/command 三处即用）。

## 持久卷：overlayfs 增量

- 卷以独立 PVC 的形式申请（使用本驱动的 StorageClass），并通过 `persistentVolumeClaim` 引用挂载（完整示例见 [`examples/pod.yaml`](examples/pod.yaml)）：

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
  # pod spec 中：
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: demo
  ```

- StorageClass 使用 `volumeBindingMode: WaitForFirstConsumer`：卷只在使用该 PVC 的 Pod 被调度之后才供给。调度器把选中的节点写进 PVC 的 `volume.kubernetes.io/selected-node` 注解，该节点上的驱动实例随后创建预绑定的 PV `overlayfs-<pvc-uid>`（`nodeAffinity` 把卷钉在该节点）。因此 PVC 的数据落在该节点的本地磁盘上，之后挂载同一 PVC 的所有 Pod 都会被调度回同一节点。单节点集群无需任何操作；多节点集群直接让调度器选节点即可（除非确需手动固定，否则不要设置 `nodeName`）。

- PVC 中的数据是持久的：删除并重建 Pod 后数据仍在。关于 [generic ephemeral volume](https://kubernetes.io/docs/concepts/storage/projected-volumes/#generic-ephemeral-volume) 的注意事项见下文。

> 不要用 generic ephemeral volume（pod spec 中的 `volumeClaimTemplate`）承载需要保留的数据。这类卷背后的 PVC 会随 Pod 一起删除，数据在 Pod 删除时即被销毁——不满足持久化需求。请使用上文所示的独立 PVC。

### Base 与 `.as_base`

- 只要有有效 base 存在，新 stage 的卷就是一个叠在 base 之上的 overlay 文件系统；否则从空开始。

- **两种模式下你在卷里看到的内容不同。** 没有 base 时，卷就是数据目录本身（普通 bind mount）：你写的一切都在。有 base 时，数据目录只保存你的_增量_（以及被删除 base 文件的 whiteout 项——`ls -la` 时表现为字符设备）；Pod 里看到的完整视图 = base + 增量。这一点在长期停机后尤其重要：如果停机期间 base 已过期，重新挂载的卷只显示你自己的文件——数据没有丢，只是共享层不在了（调大 `maxAgeSeconds` 可保留它）。

- PVC 数据本身的持久性与 base 无关——这是结构保证的。base 只是供_后续新_ PVC 使用的共享只读起点。

- 通过在卷里写 `.as_base` 文件，Pod 可以标记「该卷之后可用作 base」。当卷被 unstage（最后一个 Pod 卸载它）且当时不存在有效 base，当前环境——base 与增量的合并视图——会被冻结成新 base，之后新建的 PVC 自动叠加其上。若已有有效 base 则跳过固化。

  - TODO: 未来可以通过检查 Pod 的退出状态来替代该标记。

- base 携带创建时间戳，超过 `--max-age-s`（默认 2592000 秒，即 30 天）后过期。过期 base 会在后台清理，除非仍有 overlay 挂载引用它们。当因此没有任何可用 base 时，下一个卷就从零开始（直到又有卷被固化为 base）。

- 有效 TTL 可逐卷设置：给 PVC 打上 `overlayfs.csi.k8s.io/max-age-s: "<秒>"` 注解，从该卷固化的 base 即使用该 TTL 而非全局值。注解缺失或非法时回退全局 `maxAgeSeconds`。

### 底层存储

仅支持节点本地存储：base 与每个 PVC 的数据目录都位于 hostPath 存储根（Helm 值 `storageRoot`，默认 `/var/lib/overlayfs-csi`）之下——这也让「卷转 base」可以快速完成。每个节点维护自己的 base，PVC 的数据固定在其供给时所在的节点（见上文）。base 就是该路径下的普通目录，因此驱动 Pod 的重启与重新部署不会丢失它们。

存储根所在文件系统必须支持 overlayfs `upperdir`：**ext4，或启用了 `ftype=1` 的 XFS**（用 `xfs_info | grep ftype` 检查）。NFS/CIFS/vfat 不受支持——对 overlay 上层的写入（每 PVC 数据目录、VM 模式快照）在这些文件系统上会失败或行为异常。

支持任意底层卷类型并不困难。对支持高效 [卷克隆](https://kubernetes.io/docs/concepts/storage/volume-pvc-datasource/) 的 CSI，可以直接用克隆替代 overlay。

## 安装

> [!CAUTION]
> 当前完全处于实验状态，使用风险自负。

1. 编译二进制并构建 docker 镜像：

   ```
   $ cd docker
   $ cross build -r --target-dir ../target-cross
   $ cp ../target-cross/release/overlayfs-csi .
   $ docker build -t overlayfs-csi .
   ```

   构建会下载 `csi.proto` 并需要 protobuf 编译器。使用 `cross` 时，[`Cross.toml`](Cross.toml) 指向 [`docker/Dockerfile.cross`](docker/Dockerfile.cross)——预装 protoc 25.1 的构建镜像（离线友好：先把 protoc 压缩包内容放到 `docker/protoc-usr/` 下）。直接在宿主机 `cargo build` 的话，自行指定 protoc：

   ```
   $ export PROTOC=/usr/local/bin/protoc PROTOC_INCLUDE=/usr/include   # 系统 protoc
   # 或复用仓库内置的：
   $ export PROTOC=$PWD/docker/protoc-usr/bin/protoc PROTOC_INCLUDE=$PWD/docker/protoc-usr/include
   ```

2. 按需修改 [Helm chart](https://helm.sh/) 的值（`chart/values.yaml`）：`namespace`、`image`、驱动 `name`、`storageClassName`（缺省用驱动名）、`storageRoot`、`maxAgeSeconds`（可被逐卷覆盖，见上文）以及 `webhook`（默认开启——`webhook.enabled: false` 可关闭 VM 模式）。
3. 安装 chart
   ```
   $ helm install overlayfs-csi chart
   ```

### 上手验证

**VM 模式**——apply [`examples/dev-pod.yaml`](examples/dev-pod.yaml)，装点东西，删除，重建：

```
$ kubectl apply -f examples/dev-pod.yaml && kubectl wait --for=condition=Ready pod/dev-cpp-01
$ kubectl exec dev-cpp-01 -- bash -c 'echo demo > /etc/hello && apt-get update -qq && apt-get install -y -qq figlet'
$ kubectl delete pod dev-cpp-01 && kubectl apply -f examples/dev-pod.yaml
$ kubectl exec dev-cpp-01 -- ls /etc/hello      # 还在
$ kubectl exec dev-cpp-01 -- which figlet       # 还装着
```

**PVC 卷**——apply [`examples/pod.yaml`](examples/pod.yaml)，创建一个 PVC 并启动挂载到 `/test` 的 Pod：

```
$ kubectl apply -f examples/pod.yaml
$ kubectl exec test -- touch /test/hello /test/.as_base
$ kubectl delete pod test && kubectl apply -f examples/pod.yaml   # 数据仍在
$ kubectl exec test -- ls /test
hello
```

上一步的 Pod 被删除后，其 `.as_base` 标记使该卷在 unstage 时被固化为新 base（假设当时无有效 base）。之后再新建 PVC 会自动叠加其上——因此直接就有 `hello`。

### 长期停机（数天到数月）

同样的示例可以经受任意长的停机——被删除的只有 Pod，PVC（及其在节点上的数据目录）或 VM 快照目录始终保留：

```
$ kubectl delete pod test        # 只删 Pod —— 切勿 kubectl delete -f examples/pod.yaml，那会连 PVC 一起删除
$ kubectl apply -f examples/pod.yaml      # 数天后：同名 PVC 复用，所有变更原样回来
```

PVC 卷：`maxAgeSeconds` 决定停机后_完整_环境（base + 你的变更）能回粘多久；PVC 数据本身不依赖它。VM Pod：注解值_就是_快照保留期。两者都设得大于预期停机间隔即可（见 [`chart/values.yaml`](chart/values.yaml)）。

## 实现细节

- 单个 Rust 二进制实现 Identity 与 Node CSI 服务（外加一个 Controller 桩——其 RPC 全部 unimplemented，供给刻意绕过 CSI Controller 服务，见下文）。kubelet 通过 UNIX socket 与 Node 服务通信。同一二进制还托管 VM 模式的 admission webhook（HTTPS，`--webhook-addr/--webhook-cert/--webhook-key`）。
- DaemonSet 每节点运行一个实例，遵循 Kubernetes CSI 设计。`CSIDriver` 对象只声明 `Persistent` 卷生命周期模式（不支持 inline ephemeral 卷）与 `STAGE_UNSTAGE_VOLUME` 节点能力。
- 每个实例拥有一个 hostPath 存储根（`--bases` 参数，即 `storageRoot` 值），其下四个子目录：`bases/`（共享只读 base 树）、`volumes/<pv-name>/`（每 PVC 数据目录）、`work/<pv-name>/`（overlay work 目录）、`vm/<namespace>/<pod-name>/`（VM 模式快照目录，内含记录 `{ttl_s, last_seen}` 的 `meta.json`）。
- 供给不经过 CSI Controller 服务，而是由每个节点的 controller 循环 watch 所有 PVC 完成：
  - 处于 `Pending`、`storageClassName` 匹配本节点 StorageClass、且 `volume.kubernetes.io/selected-node` 注解等于本节点的 PVC，会得到预绑定 PV `overlayfs-<pvc-uid>`（打标 `app.kubernetes.io/managed-by: overlayfs-csi`、回收策略 `Delete`、node affinity 钉在本节点）。操作幂等，30 秒 reconcile 循环在重启后自动重放。
  - PVC 被删除会触发删除对应 PV，并移除本节点上相应的数据目录与 work 目录。
- 挂载按声明的 `STAGE_UNSTAGE_VOLUME` 能力分两阶段：
  - `NodeStageVolume` 把卷数据目录（作为 upper 层，配一个 `work/` 目录）overlay 挂载到有效 base 之上、挂到 staging 路径；没有有效 base 时则直接 bind mount 数据目录。
  - `NodePublishVolume` 把 staging 路径 bind mount 进 Pod；`NodeUnpublishVolume`/`NodeUnstageVolume` 按相反顺序幂等 umount。
- `NodeUnstageVolume` 承担前述 base 固化：若卷内存在 `.as_base` 且无有效 base，当前视图会被拷入 `bases/<uuid>/` 并打上新鲜时间戳（先用 `cp --reflink=auto`，失败时回滚目标目录）。合并视图包含卷 stage 时所用的 base——在卸载 staging 挂载之前从 `/proc/self/mountinfo` 读取，因此即使它的 TTL 已过期，其内容仍会并入固化的树。stage 决策只评估一次并被固化复用，以避免并发 unstage 把无关卷的数据合并进被固化的 base。若 PVC 带有 `max-age-s` 注解，该卷级 TTL 会写入新 base。
- 卷的数据目录根下可能出现两个驱动管理的文件：`.as_base`（Pod 写的固化标记，被消费后移除）和 `.ofcsi-max-age-s`（驱动在 stage 时记录的卷级 TTL）。目录里其余内容都归你。
- 每个节点的后台 janitor（30 秒间隔）承担：
  - base TTL 清理：超过_有效_ TTL 的 base 会被移除——有效 TTL 取 base 内嵌的卷级值，无内嵌则取全局 `--max-age-s`；除非 `/proc/self/mountinfo` 显示仍有 overlay 挂载的 `lowerdir` 指向它们（通过解析 mountinfo 做引用检测）。
  - 孤儿 GC：没有对应 managed PV 的 `volumes/` 目录——且超过 5 分钟宽限期——会被移除；`work/` 目录在其对应卷目录不存在时同样被移除。
- 若某次 `/proc/self/mountinfo` 快照解析出零个 overlay 挂载，janitor 会记录告警并跳过整轮清理——因为此时的引用检查无法保证不会静默放行。
- janitor 同时承担 VM 模式 GC（`vm_gc_once`，不依赖 mountinfo）：带 VM 注解但缺少注入卷的 Pod（webhook 停机期间建成）会被删除以便重建时注入；活跃 VM Pod 的快照目录会刷新 `last_seen`；无活跃 Pod 的快照目录在其记录的 TTL 届满后删除——`meta.json` 缺失或损坏时保守保留该目录。

## TODO

- 通过 Pod 状态（而非 `.as_base` 标记）推断卷能否用作 base（见上文）。
- 支持其他底层存储（见上文）。
- 通过标签区分不同类别的 base。
- VM 模式跨节点：记录 Pod 快照所在的节点（如节点注解）并把重建的 Pod 钉回该节点，取代目前的单节点调度假设。
- VM 模式（远期）：行业正把镜像层管理推进到可插拔 containerd 快照器之后（Docker 29+ 默认化、Nydus/OverlayBD 等）。自定义快照器把容器 rootfs 直接落在持久层，可免除按目录叠加 overlay 实现整 rootfs 持久——重量级路线，本项目现阶段刻意不做。值得一提的是，VM 模式完全不依赖运行时的 overlayfs：其 lower 层是任意 bind 出来的目录视图，快照器换血不影响它。
