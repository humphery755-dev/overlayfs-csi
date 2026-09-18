# 设计：Pod 注解驱动的 VM-like 持久化（写时持久，零同步窗口）

日期：2026-09-17
状态：已确认（用户逐项确认范围/可靠性/特权/注解 API）

## 背景与目标

现状：数据持久化以 PVC 为锚点，用户须手动建 PVC、手写 volumes/volumeMounts，且只能持久化挂载点内容——不符合"像虚拟机一样用容器"的直觉。

目标：pod 打**一个注解**即获得 VM 语义——容器内任意系统级修改（`apt install`、改 `/etc`、`/usr`）**写时持久**（SIGKILL/OOM/断电不丢已确认写入，无同步窗口），pod 删除重建后内容原样回来。

非目标：多节点数据漫游（MVP 锁定单节点假设，见"已知限制"）；显式 vm-id 注解；/tmp、/run 持久化；initContainer/sidecar 持久化。

## 关键决策（用户确认）

| 决策点 | 结论 |
|---|---|
| 持久范围 | 包括系统级修改（rootfs 白名单目录），非仅数据目录 |
| 可靠性 | 写时持久；rsync 快照同步方案被否决（有崩溃丢数据窗口） |
| 特权 | 接受 privileged（内核要求：非特权无法 mount） |
| 注解 API | 单注解 `overlayfs.csi.k8s.io/max-age-s`：有值=开启，值=删除后保留 TTL 秒 |

## 原理

容器 rootfs 本身是 overlayfs。对白名单目录逐个做**自叠加 overlay**：

```
容器内 /usr = overlay( lowerdir=bind 固定的镜像 /usr, upperdir=持久 hostPath 目录 )
```

- 每次写入直接落在持久 upperdir（写穿透磁盘），没有"退出写回"动作 → 零同步窗口
- 删除 = whiteout 持久；`rm` 过的东西重建后仍是删掉的
- 镜像升级：lower 实时来自新镜像，用户修改仍叠加（whiteout 继续遮蔽同名文件）——VM 换宿主镜像的语义
- 需要 `mount --bind` 先固定镜像目录再叠加（避开 overlay 自引用/重叠检查）

## API

```yaml
metadata:
  annotations:
    overlayfs.csi.k8s.io/max-age-s: "7776000"   # 开启 + 删除后保留 90 天
spec:
  containers:
    - name: test
      image: openresty/openresty:latest
      command: ["sleep", "infinity"]   # 必须显式，否则拒绝创建（wrapper 需转发原命令）
```

vm-id 恒为 `<namespace>/<pod-name>`：同名 pod 天然被 k8s 唯一性约束挡住并发冲突，无需额外检测（YAGNI）。

## 架构（全部在现有驱动进程内）

| 组件 | 职责 |
|---|---|
| `src/webhook.rs`（新） | HTTPS server（hyper + tokio-rustls），端点 `POST /mutate`：有注解 → 校验 → JSON Patch 注入 |
| `vm-init.sh`（chart ConfigMap） | 白名单 bind+overlay 叠加、快照内 kubelet trio 清理、`exec` 原命令 |
| controller watcher 扩展 | VM 目录 GC（TTL 过期且无活跃 pod）+ 兜底：带注解但未注入的 pod → 删除重建（防静默不持久） |

数据布局（驱动 Store 下新增，hostPath `DirectoryOrCreate` 自建，watcher 不负责建目录）：

```
/var/lib/overlayfs-csi/vm/<ns>/<pod-name>/
  meta.json      # { "ttl_s": 7776000, "last_seen": <unix> } —— webhook 写 ttl；GC 循环刷 last_seen
  up/            # etc/ usr/ var/ opt/ root/ home/ srv/ 的持久 upper
  work/          # overlay workdir（与 up 同文件系统，hostPath 满足）
  bind/          # 运行时 bind 固定镜像目录的挂载点
```

注入内容（webhook 生成，用户无感）：
- 容器级：`securityContext.privileged: true`；volumeMounts `/.ofcsi-vm`（hostPath）+ `/.ofcsi-vm-bin`（ConfigMap，只读）；command 改写为 `["sh","/.ofcsi-vm-bin/vm-init.sh", <原command...>]`
- pod 级：两个 volumes；`terminationGracePeriodSeconds` 不动（无退出动作）

vm-init.sh 要点：

```sh
for d in etc usr var opt root home srv:      # 白名单
    mount --bind /$d /.ofcsi-vm/bind/$d
    mount -t overlay -o lowerdir=bind/$d,upperdir=up/$d,workdir=work/$d /$d
rm -f up/etc/{hosts,hostname,resolv.conf}    # 挂载前清：kubelet 注入的当前值必须生效
exec "$@"
```

## 错误处理（fail-fast 原则，与 PVC 注解的 warn+回退**刻意不同**）

PVC 的 max-age-s 非法时回退全局 TTL 只损失 TTL 精度；VM 模式下注解无效=用户以为持久实际没持久=数据丢失。因此**一律拒绝创建**：

| 场景 | 行为 |
|---|---|
| 注解值非正整数 | webhook 403，message 指明原因 |
| 容器缺 command | webhook 403 |
| `runAsNonRoot`/非 0 `runAsUser`（无法 mount） | webhook 403 |
| 白名单目录内有意外子挂载点（会被遮蔽；kubelet trio 除外） | vm-init.sh 启动即 exit 1 |
| webhook 不可用（failurePolicy: Ignore，不拖累普通 pod） | pod 建成但未注入 → watcher 检测到（有注解、无 `.ofcsi-vm` 特征）→ 删除该 pod 强制重走注入 |
| 同 ns 同名 VM pod 并发 | 不可能：k8s 命名唯一性 |

系统错误（mount 失败、GC IO 失败）：ERROR 日志上抛，不吞。业务预期（无快照目录=首次启动）：DEBUG，直接跑。

## 已知限制（文档明示）

1. **单节点假设**：数据在节点本地盘；多节点下重建可能调度到别的节点 → 数据"看不见"（仍在原节点）。多节点扩展路径：watcher 将本节点 vm 目录清单 annotate 到 Node，webhook 据此设 `spec.nodeName`。
2. 仅注入普通容器；initContainer/sidecar 不持久。
3. 主镜像须含 POSIX `sh`；需 root（privileged 已强制）。
4. `/tmp`、`/run` 不持久（运行时垃圾，重启干净更健康）。
5. `machine-id`、ssh host keys 持久（VM 语义：跨重建稳定）。
6. 快照体积 ≈ 白名单目录变更集 + 镜像对应目录结构，占用与镜像同量级。
7. 存储根所在文件系统必须支持 overlayfs upperdir：ext4 或 XFS(ftype=1)，不支持 NFS/CIFS/vfat。
8. overlayfs 固有行为：跨层目录 rename 返回 EXDEV（规范工具退化为拷贝+删除）；对镜像内已有文件的首次写触发完整 copy-up，之后零开销。e2e（apt/dpkg）未见影响。

## 远期方向（行业佐证，现阶段不做）

行业正把镜像层管理推进到可插拔 containerd 快照器（Docker 29+ 默认化、Nydus/OverlayBD 等）。自定义快照器把容器 rootfs 直接落在持久层可免除按目录叠加，实现整 rootfs 持久——重量级路线。注意：本方案的 lower 层是 bind 出来的任意目录视图，**不依赖运行时的 overlayfs 实现**，快照器演进不影响其正确性。

## 测试

- 单元（`src/webhook.rs` 内 `#[cfg(test)]`，沿用现有风格）：注解解析（合法/非法/缺省）、注入 patch 生成（command 改写、privileged、双 volume、trio 容器全覆盖）、未注入判定、GC 判定（过期/未过期/活跃）、meta 读写。
- 集成（root-gated，沿用现有 3 个同类测试模式）：真 fs 上 `bind + overlay 自叠加` 流程——写入落 upper、whiteout 生效、重挂恢复。
- e2e（集群，交付后跑）：apply 带注解 pod → exec 装软件 → delete → apply → 验证修改还在。
