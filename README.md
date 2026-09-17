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

### Bases and `.as_base`

- Whenever a valid base is available, a newly staged volume is an overlay filesystem on top of it. Otherwise, it starts empty.

- The PVC data itself is persistent by construction, independently of bases. A base is only a shared read-only starting point for _subsequent, new_ PVCs.

- By writing a `.as_base` file on the volume, a pod can indicate that the volume can later be used as a _base_ for subsequent volumes. When the volume is unstaged (its last pod unmounted it) and no valid base exists at that moment, the current environment — the base overlaid with the volume's increments, merged into a single tree — is frozen into a new base, which new PVCs will then layer on. If a valid base already exists, the promotion is skipped.

  - TODO: This could be replaced by a check on the pod exit status.

- Bases carry a creation timestamp and expire after `--max-age-s` (2592000 s, i.e. 30 days, by default). Expired bases are cleaned up in the background unless an overlay mount still references them. When this results in no base being available, the next volume simply starts from scratch (until some volume is promoted again).

- The effective TTL can be set per volume: annotate the PVC with `overlayfs.csi.k8s.io/max-age-s: "<seconds>"`, and the base promoted from that volume uses that TTL instead of the global value. Invalid or missing annotations fall back to the global `maxAgeSeconds`.

### Underlying storage

Only node-local storage is supported: bases and per-PVC data directories live under a hostPath storage root (`storageRoot` Helm value, `/var/lib/overlayfs-csi` by default), which in particular allows quickly converting volumes to bases. Each node maintains its own bases, and a PVC's data lives on the node it was provisioned on (see above). Bases are ordinary directories under this path, so driver pod restarts and redeployments do not lose them.

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

2. Customize values in the [Helm chart](https://helm.sh/) (`chart/values.yaml`): driver name, `storageClassName` (defaults to the driver name), `storageRoot`, `maxAgeSeconds`.
3. Apply the chart
   ```
   $ helm install overlayfs-csi chart
   ```

To test the deployment, apply [`pod.yaml`](pod.yaml), which creates a PVC and a pod mounting it at `/test`:

1. Create the namespace if needed, then apply the example
   ```
   $ kubectl create namespace overlayfs-csi --dry-run=client -o yaml | kubectl apply -f -
   $ kubectl apply -f pod.yaml
   ```
2. Write some data and mark the volume as base material
   ```
   $ touch /test/hello
   $ touch /test/.as_base
   ```
3. Delete the pod, then apply `pod.yaml` again: the pod remounts the same PVC and the data is still there.
   ```
   $ ls /test
   hello
   $ touch /test/hi
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

- A single Rust binary implements the required Identity, Node and Controller CSI services. Kubelet communicates with the Node service using a UNIX socket.
- A daemonset runs one such server per node, following the Kubernetes CSI design. The `CSIDriver` object advertises only the `Persistent` volume lifecycle mode (inline ephemeral volumes are not supported) and the `STAGE_UNSTAGE_VOLUME` node capability.
- Each server has a hostPath storage root (`--bases`, the `storageRoot` value) with three subdirectories: `bases/` (shared read-only base trees), `volumes/<pv-name>/` (per-PVC data directories) and `work/<pv-name>/` (overlay work directories).
- Provisioning is not done through the CSI Controller service, but by a controller loop in each node's server that watches all PVCs:
  - A PVC that is `Pending`, whose `storageClassName` matches this node's StorageClass and whose `volume.kubernetes.io/selected-node` annotation equals this node gets a pre-bound PV `overlayfs-<pvc-uid>` (labelled `app.kubernetes.io/managed-by: overlayfs-csi`, reclaim policy `Delete`, node affinity to this node). The operation is idempotent, and a 30 s reconcile loop replays it after restarts.
  - A deleted PVC triggers deletion of its PV and removal of the corresponding data and work directories on this node.
- Mounting is two-phase, as required by the advertised `STAGE_UNSTAGE_VOLUME` capability:
  - `NodeStageVolume` either overlay-mounts the volume data directory (as upper layer, with a `work/` directory) on top of a valid base onto the staging path, or — when no valid base exists — bind-mounts the data directory itself.
  - `NodePublishVolume` bind-mounts the staging path into the pod; `NodeUnpublishVolume`/`NodeUnstageVolume` umount (idempotently) in reverse order.
- `NodeUnstageVolume` performs the base promotion described above: if the volume contains `.as_base` and no valid base exists, the current view is copied into `bases/<uuid>/` with a fresh timestamp (`cp --reflink=auto` first, with rollback of the destination on failure). The merged view includes the base the volume was staged on, read from `/proc/self/mountinfo` before the staging mount is removed — so it joins the frozen tree even if its TTL has since expired. The stage decision is evaluated once and reused for the promotion, to avoid a concurrent unstage merging unrelated volume data into the frozen base.
- A background janitor (30 s interval) runs on each node:
  - Base TTL cleanup: bases whose timestamp is older than `--max-age-s` are removed, unless `/proc/self/mountinfo` shows an overlay mount whose `lowerdir` still points at them (reference detection by parsing mountinfo).
  - Orphan GC: `volumes/` directories without a matching managed PV — and older than a 5 minute grace period — are removed, as are `work/` directories whose volume directory no longer exists.
- If a snapshot of `/proc/self/mountinfo` parses to zero overlay mounts, the janitor logs a warning and skips the entire cleanup round, since the reference checks could not be trusted not to silently pass.

## TODOs

- Deduce whether a volume can be used as base from the pod status (see above).
- Support other underlying storages (see above).
- Allow different categories of bases with labels.
