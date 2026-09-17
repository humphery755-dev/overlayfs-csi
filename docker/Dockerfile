FROM debian:bullseye-slim

# 运行时依赖（bullseye-slim 自带，显式记录）：
#   mount/umount（util-linux）—— stage/publish/unstage 的 overlay/bind 挂载
#   cp（coreutils，--reflink=auto）—— .as_base 固化时的合并视图拷贝
# 容器须以 privileged 运行（chart 已配置），mount 系统调用才可用。
COPY overlayfs-csi /usr/local/bin/csi

ENTRYPOINT ["/usr/local/bin/csi"]
