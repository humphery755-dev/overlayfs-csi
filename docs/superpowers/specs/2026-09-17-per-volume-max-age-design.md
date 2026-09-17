# PVC 级 maxAgeSeconds 覆盖全局 设计

日期：2026-09-17
状态：用户已确认

## 需求

`maxAgeSeconds`（base TTL）默认取全局（chart values → `--max-age-s`）；pod.yaml（PVC 层）有配置时覆盖全局。

## 语义（用户已拍板）

PVC 注解 `overlayfs.csi.k8s.io/max-age-s: "<秒>"` → **该卷固化出的 base 用此 TTL**。TTL 从全局参数变为 base 自身属性，全局值只是缺省——所有卷对同一 base 的有效性判定保持一致（base 自包含 TTL）。

## 数据流

1. 用户在 PVC `metadata.annotations` 写 `overlayfs.csi.k8s.io/max-age-s: "7776000"`；
2. provision（`build_pv`）：合法值（正整数）写入 PV 的 `csi.volume_attributes["maxAgeSeconds"]`；缺失/非法 → 不写 + WARN；
3. kubelet 把 PV `volume_attributes` 作为 `volume_context` 透传进 **`NodeStageVolume`** 请求（实现修正：`NodeUnstageVolumeRequest` 无 volume_context 字段，无法透传）；
4. stage：从 `volume_context` 解析 TTL，持久化到数据目录元文件 `volumes/{id}/.ofcsi-max-age-s`（PV 的 volume_attributes 不可变，元文件不会陈旧）；
5. unstage 固化：读元文件（缺失/损坏 → WARN + 回退全局）→ `promote_to_base_with_mount(..., ttl)` 写入新 base。

## base 自包含 TTL（数据格式）

`.as_base` 文件单行（RFC3339 时间戳）→ 两行（时间戳 + TTL 秒）：
- `Base::write_time(ttl: Option<i64>)`：ttl 为 Some 时写第二行；
- `Base::ttl() -> Option<i64>`：读第二行；旧单行格式/缺第二行 → `None`；
- `Base::valid(全局值)`：`ttl().unwrap_or(全局值)` 作为有效 TTL —— **旧格式 base 天然兼容**。

`cleanup_once` 的 `base.valid(self.max_age_s)` 调用点签名不变；stage 侧 `find_valid_base` 不变（判定基于 base 自身）。

## 错误处理

注解非数字/≤0 → provision WARN + 回退全局（不 fail）；volume_context 解析失败 → 固化 WARN + 回退；base 文件第二行损坏 → `ttl()` 返回 None 回退全局。

## 测试

- `build_pv`：有注解 → volume_attributes 含值；无注解/非法 → 空；
- `Base`：两行格式读写、`ttl()` 解析、旧单行回退全局、valid 用内嵌 TTL（构造时间差，不依赖 sleep）；
- `promote_to_base_with_mount(..., ttl)` → dst 的 `ttl()` 正确。

全部无 root 可跑。

## 文档

README 中英补注解用法；values.yaml 注释补「可被 PVC 注解逐卷覆盖」。pod.yaml 不动（避免与用户编辑冲突）。
