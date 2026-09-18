# overlayfs-csi

English | [简体中文](README.zh-CN.md)

This implements a Kubernetes [Container Storage Interface](https://github.com/container-storage-interface/spec/blob/master/spec.md) that provides persistent volumes as [overlay mounts](https://en.wikipedia.org/wiki/OverlayFS) on top of shared _base_ volumes. Only new and modified files are written to a volume's own data directory; everything it has in common with a base is shared read-only.

This can be particularly useful in build pipelines, as it allows benefiting from incremental compilation (whenever supported) without having to copy all files for each run.

This is similar to using `dataSource` in CSIs that support efficient [volume cloning](https://kubernetes.io/docs/concepts/storage/volume-pvc-datasource/), except that maintaining base volumes is handled by the CSI automatically.

This repository also provides an example for building Kubernetes CSIs in Rust.

## Usage

- Volumes are requested as standalone [PVCs](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) using this driver's StorageClass, and mounted through a `persistentVolumeClaim` reference (see [`pod.yaml`](pod.yaml) for a complete example):

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
  # in the pod spec:
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: demo
  ```

- The StorageClass uses `volumeBindingMode: WaitForFirstConsumer`: the volume is provisioned only once a pod using the PVC has been scheduled. The scheduler records the chosen node in the `volume.kubernetes.io/selected-node` annotation of the PVC, and the driver instance on that node creates a pre-bound PV named `overlayfs-<pvc-uid>` whose `nodeAffinity` pins the volume there. The PVC's data therefore lives on that node's local disk, and every later pod mounting the same PVC is scheduled onto the same node. In single-node clusters nothing needs to be done; in multi-node clusters just let the scheduler pick the node (do not set `nodeName` unless you want to pin it manually).

- Data in a PVC is persistent: it survives pod deletion and recreation. See the caveat about [generic ephemeral volumes](https://kubernetes.io/docs/concepts/storage/projected-volumes/#generic-ephemeral-volume) below.

> [!WARNING]
> Do not use generic ephemeral volumes (`volume.csi` with a `volumeClaimTemplate` in the pod spec) with this driver for anything you want to keep. Such volumes are backed by a PVC that is deleted together with the pod, so their data is destroyed on pod deletion — they do not satisfy persistence needs. Use a standalone PVC as above.

## VM mode: pod-wide persistence via a single annotation

Annotate a pod with `overlayfs.csi.k8s.io/max-age-s: "<seconds>"` and it behaves like a small VM: anything you change inside the container — `apt install`, edited configs — persists across pod deletion and recreation, for the whole system tree (`/etc`, `/usr`, `/var`, `/opt`, `/root`, `/home`, `/srv`). No PVC required:

```yaml
metadata:
  name: vm-demo
  annotations:
    overlayfs.csi.k8s.io/max-age-s: "7776000"   # keep the snapshot for 90 days after deletion
spec:
  containers:
    - name: vm-demo
      image: debian:bullseye-slim
      command: ["sleep", "infinity"]   # an explicit command is required
```

A mutating webhook (deployed by the chart) rewrites annotated pods at creation time:

- Each system directory above is layered with an overlay whose `upperdir` lives in a hostPath snapshot dir (`<storageRoot>/vm/<namespace>/<pod-name>`); the image content is pinned as the lower layer via a bind mount. Every write goes straight to disk in the snapshot dir — **there is no save-on-exit step**, so SIGKILL/OOM/power loss cannot lose confirmed writes.
- On recreation the same upperdirs are mounted again: your changes are back. Deleted files stay deleted (whiteouts persist). Recreating with a newer image keeps your changes layered on top of the new content.
- The annotation value doubles as the retention TTL: after the pod is gone, the controller's GC removes the snapshot dir once this many seconds have passed.
- The webhook also injects `privileged: true` (mounting requires it) and wraps each container's command with a small init script that performs the mounts and then re-execs your command.

Hard rules (fail-fast by design — silent non-persistence would mean silent data loss):

- An invalid annotation value, a missing `command`, or `runAsNonRoot`/non-root-uid pods are **refused at creation** with an explanatory message.
- If the webhook is briefly unavailable (its `failurePolicy` is `Ignore`, so ordinary pods are never blocked), the controller detects VM pods that were created without injection and deletes them so they are recreated with injection.
- Unusual mount points inside the system directories (they would be shadowed) make the init script fail the container at startup instead of hiding them.

Known limits: only plain containers are persisted (not initContainers/sidecars); the image must contain a POSIX `sh`; `/tmp` and `/run` are not persisted while `machine-id` and ssh host keys are (VM semantics); snapshots are node-local, so multi-node clusters need the pod to be rescheduled onto the same node. Two inherited overlayfs behaviours to be aware of: `rename` of a directory across layers returns `EXDEV` (well-behaved tools like `mv` and `git` fall back to copy+delete; very large directories take one extra copy); and the *first* write to an existing file from the image triggers a full copy-up of that file (subsequent writes have no extra cost).

### Bases and `.as_base`

- Whenever a valid base is available, a newly staged volume is an overlay filesystem on top of it. Otherwise, it starts empty.

- **What you see inside the volume differs between the two modes.** Without a base, the volume is the data directory itself (a plain bind mount): it contains everything you wrote. With a base underneath, the data directory holds only your *increments* (plus whiteout entries for deleted base files — they appear as character devices when you `ls -la`); the full view you get in the pod is base + increments. This matters after long downtime: if the base has expired by then, a remounted volume shows only your own files — nothing is lost, the shared layer is simply gone (raise `maxAgeSeconds` to keep it).

- The PVC data itself is persistent by construction, independently of bases. A base is only a shared read-only starting point for _subsequent, new_ PVCs.

- By writing a `.as_base` file on the volume, a pod can indicate that the volume can later be used as a _base_ for subsequent volumes. When the volume is unstaged (its last pod unmounted it) and no valid base exists at that moment, the current environment — the base overlaid with the volume's increments, merged into a single tree — is frozen into a new base, which new PVCs will then layer on. If a valid base already exists, the promotion is skipped.

  - TODO: This could be replaced by a check on the pod exit status.

- Bases carry a creation timestamp and expire after `--max-age-s` (2592000 s, i.e. 30 days, by default). Expired bases are cleaned up in the background unless an overlay mount still references them. When this results in no base being available, the next volume simply starts from scratch (until some volume is promoted again).

- The effective TTL can be set per volume: annotate the PVC with `overlayfs.csi.k8s.io/max-age-s: "<seconds>"`, and the base promoted from that volume uses that TTL instead of the global value. Invalid or missing annotations fall back to the global `maxAgeSeconds`.

### Underlying storage

Only node-local storage is supported: bases and per-PVC data directories live under a hostPath storage root (`storageRoot` Helm value, `/var/lib/overlayfs-csi` by default), which in particular allows quickly converting volumes to bases. Each node maintains its own bases, and a PVC's data lives on the node it was provisioned on (see above). Bases are ordinary directories under this path, so driver pod restarts and redeployments do not lose them.

The filesystem holding the storage root must support overlayfs `upperdir`: **ext4, or XFS with `ftype=1`** (check with `xfs_info | grep ftype`). NFS/CIFS/vfat are not supported — writes into overlay upper layers (per-PVC data, VM-mode snapshots) would fail or silently misbehave there.

It would be fairly easy to support arbitrary volume types. For CSIs that support efficient [volume cloning](https://kubernetes.io/docs/concepts/storage/volume-pvc-datasource/), these could be used instead of the overlays.

## Installation

> [!CAUTION]
> This is for now completely experimental, use at your own risk.

1. Compile the binary and build the docker image:

   ```
   $ cd docker
   $ cross build -r --target-dir ../target-cross
   $ cp ../target-cross/release/overlayfs-csi .
   $ docker build -t overlayfs-csi .
   ```

   The build downloads `csi.proto` and needs a protobuf compiler. With `cross`, [`Cross.toml`](Cross.toml) points at [`docker/Dockerfile.cross`](docker/Dockerfile.cross), a builder image with protoc 25.1 preinstalled (useful offline: place the protoc tarball contents under `docker/protoc-usr/` first). For a plain `cargo build` on the host, point the toolchain at a protoc yourself:

   ```
   $ export PROTOC=/usr/local/bin/protoc PROTOC_INCLUDE=/usr/include   # system protoc
   # or reuse the vendored one:
   $ export PROTOC=$PWD/docker/protoc-usr/bin/protoc PROTOC_INCLUDE=$PWD/docker/protoc-usr/include
   ```

2. Customize values in the [Helm chart](https://helm.sh/) (`chart/values.yaml`): `namespace`, `image`, driver `name`, `storageClassName` (defaults to the driver name), `storageRoot`, `maxAgeSeconds` (per-volume overridable, see above).
3. Apply the chart
   ```
   $ helm install overlayfs-csi chart
   ```

To test the deployment, apply [`pod.yaml`](pod.yaml), which creates a PVC and a pod mounting it at `/test` (both in the `kube-system` namespace, matching the chart default):

1. Apply the example (the namespace already exists in most clusters; adjust it in `pod.yaml` if yours differs)
   ```
   $ kubectl apply -f pod.yaml
   ```
2. Write some data and mark the volume as base material
   ```
   $ kubectl exec test -- touch /test/hello
   $ kubectl exec test -- touch /test/.as_base
   ```
3. Delete the pod, then apply `pod.yaml` again: the pod remounts the same PVC and the data is still there.
   ```
   $ kubectl exec test -- ls /test
   hello
   $ kubectl exec test -- touch /test/hi
   ```
   Writes go to the volume's own data directory only. A base — if one exists — is never modified, and no untouched file is copied.
4. Once the pod from step 2 was deleted, its `.as_base` marker caused the volume to be frozen into a new base on unstage (assuming no valid base existed). A _new_ PVC created afterwards starts as an overlay on top of it and therefore already contains `hello`.

### Shutting down for a long time (days to months)

The same example survives arbitrarily long downtime — only the pod is deleted, the PVC (and with it the data directory on the node) stays:

```
$ kubectl delete pod test        # pod only — never `kubectl delete -f pod.yaml`, that would delete the PVC too
$ kubectl apply -f pod.yaml      # days later: same PVC reused, all changes are still there
```

`maxAgeSeconds` controls how long the _full_ environment (base + your changes) re-attaches after downtime; PVC data itself does not depend on it. Set it larger than your expected downtime interval (see [`chart/values.yaml`](chart/values.yaml)).

## Implementation details

- A single Rust binary implements the Identity and Node CSI services (plus a Controller stub whose RPCs are all unimplemented — provisioning deliberately bypasses the CSI Controller service, see below). Kubelet communicates with the Node service using a UNIX socket. The same binary hosts the VM-mode admission webhook (HTTPS, `--webhook-addr/--webhook-cert/--webhook-key`).
- A daemonset runs one such server per node, following the Kubernetes CSI design. The `CSIDriver` object advertises only the `Persistent` volume lifecycle mode (inline ephemeral volumes are not supported) and the `STAGE_UNSTAGE_VOLUME` node capability.
- Each server has a hostPath storage root (`--bases`, the `storageRoot` value) with four subdirectories: `bases/` (shared read-only base trees), `volumes/<pv-name>/` (per-PVC data directories), `work/<pv-name>/` (overlay work directories) and `vm/<namespace>/<pod-name>/` (VM-mode snapshot dirs with a `meta.json` recording `{ttl_s, last_seen}`).
- Provisioning is not done through the CSI Controller service, but by a controller loop in each node's server that watches all PVCs:
  - A PVC that is `Pending`, whose `storageClassName` matches this node's StorageClass and whose `volume.kubernetes.io/selected-node` annotation equals this node gets a pre-bound PV `overlayfs-<pvc-uid>` (labelled `app.kubernetes.io/managed-by: overlayfs-csi`, reclaim policy `Delete`, node affinity to this node). The operation is idempotent, and a 30 s reconcile loop replays it after restarts.
  - A deleted PVC triggers deletion of its PV and removal of the corresponding data and work directories on this node.
- Mounting is two-phase, as required by the advertised `STAGE_UNSTAGE_VOLUME` capability:
  - `NodeStageVolume` either overlay-mounts the volume data directory (as upper layer, with a `work/` directory) on top of a valid base onto the staging path, or — when no valid base exists — bind-mounts the data directory itself.
  - `NodePublishVolume` bind-mounts the staging path into the pod; `NodeUnpublishVolume`/`NodeUnstageVolume` umount (idempotently) in reverse order.
- `NodeUnstageVolume` performs the base promotion described above: if the volume contains `.as_base` and no valid base exists, the current view is copied into `bases/<uuid>/` with a fresh timestamp (`cp --reflink=auto` first, with rollback of the destination on failure). The merged view includes the base the volume was staged on, read from `/proc/self/mountinfo` before the staging mount is removed — so it joins the frozen tree even if its TTL has since expired. The stage decision is evaluated once and reused for the promotion, to avoid a concurrent unstage merging unrelated volume data into the frozen base. If the PVC carried a `max-age-s` annotation, that per-volume TTL is written into the new base.
- Two driver-managed files may appear at the root of a volume's data directory: `.as_base` (the promotion marker a pod writes, removed after it is consumed) and `.ofcsi-max-age-s` (the per-volume TTL the driver records at stage time). Everything else in the directory is yours.
- A background janitor (30 s interval) runs on each node:
  - Base TTL cleanup: bases older than their *effective* TTL — the per-volume value embedded in the base, or the global `--max-age-s` when it has none — are removed, unless `/proc/self/mountinfo` shows an overlay mount whose `lowerdir` still points at them (reference detection by parsing mountinfo).
  - Orphan GC: `volumes/` directories without a matching managed PV — and older than a 5 minute grace period — are removed, as are `work/` directories whose volume directory no longer exists.
- If a snapshot of `/proc/self/mountinfo` parses to zero overlay mounts, the janitor logs a warning and skips the entire cleanup round, since the reference checks could not be trusted not to silently pass.
- The janitor also runs the VM-mode GC (`vm_gc_once`, independent of mountinfo): pods carrying the VM annotation but missing the injected volumes (webhook was down, `failurePolicy: Ignore`) are deleted so they get recreated with injection; snapshot dirs of active VM pods get their `last_seen` refreshed; snapshot dirs without an active pod are removed once their recorded TTL has elapsed — a missing or corrupt `meta.json` makes the dir conservatively kept.

## TODOs

- Deduce whether a volume can be used as base from the pod status (see above).
- Support other underlying storages (see above).
- Allow different categories of bases with labels.
- VM mode across nodes: record which node holds a pod's snapshot (e.g. node annotations) and pin recreated pods there, instead of relying on single-node scheduling.
- VM mode, long term: the industry is moving image-layer management behind pluggable containerd snapshotters (Docker 29+ defaults, Nydus/OverlayBD, …). A custom snapshotter mounting the container rootfs directly on a persistent layer would make the whole rootfs durable without the per-directory overlay — a heavyweight path this project deliberately avoids for now. Notably, VM mode does not depend on the runtime's overlayfs at all: its lower layer is any bind-mounted directory view, so snapshotter swaps do not affect it.
