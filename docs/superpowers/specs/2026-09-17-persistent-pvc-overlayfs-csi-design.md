# overlayfs-csi 持久卷改造设计

日期：2026-09-17
状态：已与用户逐节确认

## 背景与目标

当前 overlayfs-csi 的数据流是「emptyDir + base TTL 缓存」：卷的 upper 层落在动态创建的 blank pod 的 emptyDir 里，pod 删除即随 kubelet 回收；唯一跨 pod 的持久化路径是 `.as_base` 提升为 base（且 base 有 TTL，driver pod 重启即全丢）。

目标：改为**真正的底层持久卷**——pod 内 `apt install` 等对任意目录的变更，在 pod 删除重建后依然存在；同时保留 overlay+base 的增量构建缓存价值。

## 硬约束

1. **性能零损失**：写路径必须保持在节点本地盘 overlayfs（与现状相同 IO 路径）。任何网络存储（NFS/Ceph/Longhorn）后端直接排除。
2. 标准接口：数据身份 = 独立 PVC（generic ephemeral volume 的 PVC 随 pod 级联回收，不适用；StatefulSet `volumeClaimTemplate` 天然生成独立 PVC，同样适用）。
3. 多节点正确性：数据固定在节点本地盘，pod 重建必须回到原节点。
4. 废弃 inline ephemeral 模式，只走 PVC。

## 关键机制发现（简化设计的前提）

1. **调度器代选节点**：`WaitForFirstConsumer` 下，kube-scheduler 为业务 pod 选定节点后在 PVC 上打注解 `volume.kubernetes.io/selected-node`。被选中节点的 driver 实例（DaemonSet）直接本地建目录即可，**不需要 helper pod、不需要 leader election**。现有借 blank pod 分配存储的整套机制（`create_pod`/`data_pod.yaml`/`POD_ID`）删除。
2. **确定性命名消竞态**：PV 名 = `overlayfs-<pvc-uid>`；多节点 watcher 各自只处理 `selected-node == 本节点` 的 PVC；创建全部幂等（`AlreadyExists` 视为成功）。

## 存储布局

hostPath 根目录 `/var/lib/overlayfs-csi/`（`DirectoryOrCreate`），取代现有 `bases` emptyDir：

```
/var/lib/overlayfs-csi/
├── bases/{base-id}/      # 共享只读层（lowerdir），每节点一份；.as_base 文件内为创建时间戳（RFC3339）
├── volumes/{volume-id}/  # PVC 数据目录 = overlay upperdir，持久，pod 删除不动
└── work/{volume-id}/     # overlay workdir，unstage 时删除
```

- 三个子树同一文件系统 → overlayfs upper/work 同 fs 约束天然满足。
- bases 持久化顺带修复现有缺陷：driver pod 重启不再清空 base。
- `volume-id` = PV 名；所有路径可由 volume-id 推导，实例间零共享状态。

## CSI 接口变化

| RPC | 现状 | 新设计 |
|---|---|---|
| `ControllerCreateVolume` / `ControllerDeleteVolume` | 无 | 新增（自实现，PVC watcher 驱动） |
| `ControllerGetCapabilities` | 无 | 声明 `CREATE_VOLUME`、`DELETE_VOLUME` |
| `NodeStageVolume` | Unimplemented | overlay（有 base）或 bind（无 base）挂数据目录到 staging |
| `NodePublishVolume` | 直接 overlay 到 target | bind staging → target（标准两段式） |
| `NodeUnstageVolume` | Unimplemented | umount staging；检测 `.as_base` 固化 base |
| `NodeUnpublishVolume` | umount + 删 blank pod | 仅 umount target |
| inline ephemeral | 支持 | 删除；`CSIDriver.volumeLifecycleModes: [Persistent]` |

Identity 服务不变。

## 生命周期数据流

### 创建（provision）

PVC watcher（kube watch，断线自动重连；每 30s 全量 reconcile 兜底）：

1. 过滤：pending PVC 且 `storageClassName` 属于本 driver 的 StorageClass；
2. `selected-node == 本节点` 时：
   - `mkdir -p /var/lib/overlayfs-csi/volumes/{volume-id}`（幂等）；
   - 创建 PV `overlayfs-<pvc-uid>`：csi 源、capacity 取 PVC `requests.storage`、`claimRef` 预绑定、`nodeAffinity` 锁定本节点、`persistentVolumeReclaimPolicy: Delete`；
3. 无 `selected-node` 不动。PV 出现后 controller-manager 自动完成绑定。

### 挂载（两段式）

- **NodeStageVolume**（每 PVC 一次）：`find_valid_base()` 有 base → `mount -t overlay -o lowerdir=bases/{id},upperdir=volumes/{vid},workdir=work/{vid} staging_path`；无 base → `mount --bind volumes/{vid} staging_path`。stage 失败返回 `internal` status 上抛 kubelet 重试。
- **NodePublishVolume**：`mount --bind staging_path target_path`（同一 staging 服务同 PVC 的多个 pod）。

### 卸载与 base 固化

- **NodeUnpublishVolume**：仅 `umount target`，不删任何数据。
- **NodeUnstageVolume**：umount staging 后检查数据目录中 `.as_base` 标记。存在**且当前无有效 base**（延续现有规则，防 base 链）→ 固化：
  1. 合并视图（旧 base + upper 增量，whiteout 已解析）临时 ro 挂载；
  2. `cp -a --reflink=auto` 到 `bases/{新 uuid}/`（reflink 在 XFS/Btrfs 近 O(1)；ext4 退化为真实拷贝。显式管理操作，不在读写热路径）；
  3. **固化开始时即写 `.as_base` 时间戳**（防 TTL cleanup 误删固化中的 base），umount 临时点，删除数据目录内 `.as_base` 防重复固化；
  4. 任一步失败 → 删除半成品目录回滚，标记保留待重试。

语义：`apt install` 成果在 PVC 中天然持久；`.as_base` 额外把环境固化为后续新 PVC 的起点（增量构建缓存价值保留）。

### 删除

PVC `Deleted` 事件 → API 删除对应 PV（自建 PV 无自定义 finalizer）→ 本节点实例 `rm -rf volumes/{vid}`、`work/{vid}`。`pvc-protection` finalizer 保证事件时已无 pod 引用、staging 已 unstage。

### base TTL cleanup（保留 + 一处修复）

每 30s 扫描 `bases/`，过期（`max_age_s`，默认 86400）即删。**修复**：引用判定从内存 `mapping` 改为读 `/proc/self/mountinfo`（检查挂载的 `lowerdir` 是否指向该 base）——持久化后内存 mapping 在 driver 重启时丢失，会把引用中的 base 误删。

## 错误处理

| 场景 | 处理 | 级别 |
|---|---|---|
| provision 步骤失败 | ERROR 日志 + 重试队列；30s reconcile 再触发 | ERROR |
| stage/publish 失败 | `internal` status 上抛，kubelet 重试；无 fallback | ERROR |
| base 固化中途失败 | 删半成品回滚，`.as_base` 保留 | ERROR |
| driver 宕机期间 PVC 删除 → 孤儿 `volumes/`、`work/` | 孤儿 GC：周期扫描，无对应 PV 且无 mountinfo 引用即删 | WARN |
| watch 断线 | kube-rs 重连 + 30s reconcile | WARN |

## 竞态与边界

- 多实例：deterministic PV 名 + 幂等创建，无竞态。
- 固化 vs cleanup：时间戳先行，cleanup 不误删。
- 固化临时挂载出现在 mountinfo → cleanup 引用判定天然覆盖。
- PVC 删除 vs 活跃挂载：`pvc-protection` 兜底。
- 同 PVC 并发 stage：沿用 `Mutex`。

## 代码结构

`src/lib.rs` 拆分：

- `controller.rs`：PVC watcher、provision、删除清理、孤儿 GC；
- `node.rs`：stage/publish/unpublish/unstage、base 固化；
- `base.rs`：Base/TTL、mountinfo 解析、cleanup。

## chart 变更

- `csi.yaml`：driver 容器新增 hostPath `/var/lib/overlayfs-csi`（替代 bases emptyDir）与 `/var/lib/kubelet/plugins`（Bidirectional，staging path 位于 plugins 下）；RBAC 瘦身——删除 `pods` 全部权限及 `nodes`/`volumeattachments`/`csinodes`/`snapshot`/`storageclasses`/`events` 残留，保留 `persistentvolumes: [get,list,watch,create,delete]`、`persistentvolumeclaims: [get,list,watch]`；`CSIDriver.volumeLifecycleModes: [Persistent]`；
- 新增 StorageClass：`provisioner: overlayfs.csi.k8s.io`、`volumeBindingMode: WaitForFirstConsumer`、`reclaimPolicy: Delete`；
- `values.yaml`：新增存储根路径等项；
- 删除 `data_pod.yaml`；`pod.yaml` 改为「独立 PVC + pod 挂载」示例；README 同步。

## 测试

1. **单元**（无 root）：路径推导、PV/PVC serde 断言、`Base::valid` TTL、mountinfo 解析。
2. **集成**（root）：overlay mount/unmount 循环、固化（reflink 与退化拷贝）、mountinfo 引用检测、孤儿 GC。
3. **e2e**（k3s，沿用 `docker save → k3s ctr import` 本地分发）：
   - PVC → pod A `apt install` → 删 pod → 重建 pod A → 数据在（核心场景）；
   - `touch .as_base` → pod B（新 PVC）看到固化环境；
   - 删 PVC → PV 与宿主目录清理；
   - driver pod 重启 → base 不丢；
   - base TTL 过期清理。
4. **性能验证**：改造前后 overlay 写路径 `fio`（顺序写 + 4k 随机写）对比，确认无回归（硬约束的证据）。
