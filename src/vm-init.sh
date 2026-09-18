#!/bin/sh
# overlayfs-csi VM 模式容器入口：把白名单系统目录叠上「持久 upperdir」的
# overlay 后 exec 原命令。写入直接落在快照卷（hostPath 持久卷），pod 删除
# 重建后修改原样回来——写时持久，没有退出写回步骤。
# 本文件随快照卷分发（webhook 注入时写盘），必须与 src/webhook.rs 的注入
# 逻辑同步演进；BASE 从脚本自身位置推导，改挂载点只需改 Rust 侧 VM_DIR。
set -eu
BASE=$(cd "$(dirname "$0")" && pwd)
DIRS="etc usr var opt root home srv"
KEEP=/tmp/.ofcsi-keep
TAB=$(printf '\t')

# ── 用户挂载排除 ────────────────────────────────────────────
# 白名单目录下已存在的挂载点一律排除在 overlay 之外：先 bind 到容器私有
# 中转位，overlay 挂好后按浅→深 bind 回原路径（mount 树上后挂的在顶层，
# 用户的挂载完好地浮在 overlay 之上）。用户挂载 = 用户显式声明这份数据
# 归自己的卷管，程序不接管、不遮蔽。
rm -rf "$KEEP"
mkdir -p "$KEEP"
: > "$KEEP/submounts"
for d in $DIRS; do
  # 匹配白名单目录自身及其下所有子路径（mountPath: /var 整目录挂载也算用户
  # 挂载，必须排除，否则 overlay 会遮蔽它）。第 1 列 = 斜杠深度；浅→深排序
  # 保证 bind 回时父路径先就位。
  awk -v d="/$d" '$5 == d || index($5, d "/") == 1 { n = gsub(/\//, "/", $5); print n "\t" $5 }' \
    /proc/self/mountinfo >> "$KEEP/submounts"
done
sort -n -u "$KEEP/submounts" -o "$KEEP/submounts"
: > "$KEEP/bindings"
i=0
while IFS="$TAB" read -r _ m; do
  i=$((i + 1))
  if [ -f "$m" ]; then : > "$KEEP/$i"; else mkdir -p "$KEEP/$i"; fi
  mount --bind "$m" "$KEEP/$i"
  printf '%s\t%s\n' "$i" "$m" >> "$KEEP/bindings"
done < "$KEEP/submounts"

# ── 上次运行写进快照的 kubelet 注入文件清理（防快照膨胀）──────
mkdir -p "$BASE/up/etc"
rm -f "$BASE/up/etc/hosts" "$BASE/up/etc/hostname" "$BASE/up/etc/resolv.conf"

# ── bind 固定镜像目录 + 自叠加 overlay ────────────────────────
# 先 bind（固定挂载前的镜像内容），再以 bind 路径为 lowerdir 挂回原目录：
# 直接以 /$d 作 lowerdir 会触发 overlayfs 的路径重叠检查。
# up/work/bind 子目录按需创建——目录布局的唯一真源在本脚本。
for d in $DIRS; do
  [ -d "/$d" ] || continue
  mkdir -p "$BASE/bind/$d" "$BASE/up/$d" "$BASE/work/$d"
  mount --bind "/$d" "$BASE/bind/$d"
  mount -t overlay overlay \
    -o "lowerdir=$BASE/bind/$d,upperdir=$BASE/up/$d,workdir=$BASE/work/$d" \
    "/$d"
done

# ── 排除的挂载点按浅→深 bind 回原路径 ─────────────────────────
while IFS="$TAB" read -r i m; do
  [ -e "$m" ] || { if [ -f "$KEEP/$i" ]; then : > "$m"; else mkdir -p "$m"; fi; }
  mount --bind "$KEEP/$i" "$m"
done < "$KEEP/bindings"

# ── 执行原命令 ──────────────────────────────────────────────
[ "$#" -ge 1 ] || { echo "ofcsi-vm: no command to exec" >&2; exit 1; }
exec "$@"
