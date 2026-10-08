# Running extractedprism on Talos Linux

Talos Linux ships its own per-node API load balancer, **KubePrism**, on `localhost:7445`. This example replaces it with extractedprism fed by a static endpoint list, so the load balancer does not depend on Talos cluster discovery ([Talos docs: Discovery](https://docs.siderolabs.com/talos/v1.12/configure-your-talos-cluster/system-configuration/discovery)).

> Validated on a 5-node Talos v1.12 control plane: the runtime swap and a cold reboot both keep the node `Ready` through extractedprism; kubelet reaches the API server via `localhost:7445` from boot, with no restarts. Worker nodes were not part of that test.

The example uses the `v1alpha1` machine config format of that release. Machine configs with a newer config contract move some of these settings into separate documents, so adapt it to the [Talos configuration reference](https://docs.siderolabs.com/talos/v1.12/reference/configuration/overview) of your version.

## Configuration

Run extractedprism as a **static pod** (`machine.pods`), not a DaemonSet. On Talos the static pod is started by the kubelet directly from the machine config, so it comes up without the API server or CNI — which is exactly what the kubelet then needs in order to bootstrap. Point the single cluster endpoint at it and disable the built-in KubePrism (both bind `localhost:7445`).

See [`machine-config-patch.yaml`](./machine-config-patch.yaml). Replace the `--endpoints` list with your control-plane node IPs. Kubernetes discovery is turned off with `--enable-discovery=false`: a static pod gets no ServiceAccount token, so the in-cluster client cannot be built, and the static list is the whole source of endpoints.

### Disabling KubePrism

Both bind `localhost:7445`, so KubePrism must be off. The example sets `machine.features.kubePrism.enabled: false`.

### Cilium

If you run Cilium as the kube-proxy replacement, point it at the same endpoint:

```yaml
k8sServiceHost: localhost
k8sServicePort: "7445"
```

### Health probes must target `127.0.0.1`

extractedprism binds its health server on `127.0.0.1`. For a `hostNetwork` pod the kubelet's default probe target is the node IP, not loopback, so the probes **must** set `host: 127.0.0.1` explicitly. Otherwise liveness fails with `connection refused` and the pod enters `CrashLoopBackOff`. The example already does this.

### Apply it as machine config, not an incremental patch

Deliver this through your machine config template and a full `apply-config`. A Talos strategic-merge patch *appends* to `machine.pods` (and `pods: []` does not clear it), and RFC6902 JSON patches are rejected on a multi-document machine config (for example when an `EthernetConfig` document is present). Rendering and applying the full config avoids both.

## Boot sequence

1. kubelet starts and reads the static pod manifest from the machine config.
2. extractedprism comes up on `localhost:7445` (`hostNetwork`, no CNI needed) and proxies to a healthy control plane from the static list.
3. kubelet reaches the API through `cluster.controlPlane.endpoint = https://localhost:7445` and retries until the proxy is ready, so the brief startup race is self-healing.
4. CNI starts and uses `localhost:7445` as well.

The container image must be available to the container runtime at boot. A normal reboot keeps it in the containerd cache; for a node wiped back to a clean state, mirror the image in a registry the node can reach at boot (or pre-load it) so the static pod can start before the API server is up.

## External access

`cluster.controlPlane.endpoint` is now node-local. Generate a separate kubeconfig that points at a routable control-plane address (a node IP or an external load balancer) for access from outside the node.
