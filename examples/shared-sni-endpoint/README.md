# Routing through a shared, SNI-routed control plane endpoint

This example shows how to point kubelet at extractedprism when the control plane is reachable only through a **shared** endpoint that routes by TLS SNI — for example an nginx-ingress in `ssl-passthrough` mode that multiplexes several clusters' API servers behind a single IP and port.

The key idea: extractedprism is a pure L4 TCP passthrough and never touches TLS. It cannot inject SNI, and it does not need to. The correct SNI is supplied by the **client** (kubelet) via the standard kubeconfig field `tls-server-name`, and extractedprism forwards the ClientHello to the shared endpoint unchanged.

## When to use this

Use this pattern when **all** of the following hold:

- The API server is fronted by a shared endpoint that selects the backend by TLS SNI (nginx `ssl-passthrough`, HAProxy SNI routing, etc.).
- You control the client's kubeconfig (kubelet, controller-manager, scheduler, kube-proxy) and can set `tls-server-name`.
- The API server certificate includes the routing hostname in its SAN list.

If instead the endpoint is **dedicated** to a single API server (a direct `host:6443`, or an ingress with a single backend), no SNI is involved at all — just list the endpoint and skip the `tls-server-name` part. SNI injection for clients you cannot reconfigure (in-cluster pods reaching the API through the service ClusterIP) is **out of scope** for a passthrough proxy; that requires TLS termination and a different component.

## Topology of this example

All addresses and names below come from reserved documentation ranges (RFC 5737, RFC 2606).

- Shared edge: nginx-ingress with `ssl-passthrough`, virtual IPs `198.51.100.10:443` and `198.51.100.11:443`, routing by SNI.
- This cluster's API server public name, which is also the SNI routing key on the edge: `api.cluster-x.example.com`.
- kubelet connects to the local extractedprism listener at `127.0.0.1:7445`.

## The connection chain

```text
kubelet ──TCP──> 127.0.0.1:7445 (extractedprism) ──TCP──> 198.51.100.10:443 (shared edge) ──> api server :6443
        └──────────────────────── one end-to-end TLS session ────────────────────────┘
                                   (extractedprism never decrypts)
```

1. kubelet opens a TCP connection to `127.0.0.1:7445` and starts a TLS handshake. Because its kubeconfig sets `tls-server-name: api.cluster-x.example.com`, the ClientHello carries `SNI=api.cluster-x.example.com`, even though the connection is to a loopback IP.
2. extractedprism accepts the connection, picks a healthy edge endpoint from its static list, dials it, and pipes raw bytes in both directions. The ClientHello, including its SNI, passes through byte-for-byte.
3. The shared edge reads the cleartext SNI from the ClientHello and routes the raw TCP stream to the correct API server backend.
4. TLS is negotiated end-to-end between kubelet and the API server. kubelet's client certificate reaches the API server directly (mTLS works), and kubelet verifies the served certificate against `api.cluster-x.example.com`. extractedprism is invisible to all of this.

## 1. extractedprism configuration

Point the static endpoint list at the **edge** virtual IPs, not at the real `api-server:6443` addresses, and disable Kubernetes discovery (see "Why discovery is disabled" below).

Command-line flags:

```text
extractedprism \
  --bind-address=127.0.0.1 \
  --bind-port=7445 \
  --endpoints=198.51.100.10:443,198.51.100.11:443 \
  --enable-discovery=false
```

Equivalent environment variables (the `EP_` prefix used by the DaemonSet):

```yaml
env:
  - name: EP_ENDPOINTS
    value: "198.51.100.10:443,198.51.100.11:443"
  - name: EP_ENABLE_DISCOVERY
    value: "false"
```

## 2. kubelet kubeconfig

The single load-bearing line is `tls-server-name`. It sets both the SNI sent on the wire and the name kubelet verifies the API server certificate against.

```yaml
apiVersion: v1
kind: Config
clusters:
  - name: local-prism
    cluster:
      server: https://127.0.0.1:7445
      tls-server-name: api.cluster-x.example.com
      certificate-authority: /var/lib/kubelet/pki/ca.crt
users:
  - name: kubelet
    user:
      client-certificate: /var/lib/kubelet/pki/kubelet-client-current.pem
      client-key: /var/lib/kubelet/pki/kubelet-client-current.pem
contexts:
  - name: default
    context:
      cluster: local-prism
      user: kubelet
current-context: default
```

The same `tls-server-name` should be set in the kubeconfigs of the other components that talk to the API server through the local proxy (controller-manager, scheduler, kube-proxy).

## 3. The shared edge (the other side, for completeness)

The edge is configured independently; extractedprism does not manage it. With nginx-ingress the controller needs `--enable-ssl-passthrough`, and an Ingress maps the SNI hostname to this cluster's API server:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: cluster-x-apiserver
  annotations:
    nginx.ingress.kubernetes.io/ssl-passthrough: "true"
    nginx.ingress.kubernetes.io/backend-protocol: "HTTPS"
spec:
  ingressClassName: nginx
  rules:
    - host: api.cluster-x.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: cluster-x-apiserver
                port:
                  number: 6443
```

## What must line up

1. The API server certificate of this cluster includes `api.cluster-x.example.com` in its SAN list. Otherwise kubelet fails verification, because it checks the served certificate against `tls-server-name`. With kubeadm this is `apiServer.certSANs`.
2. The edge routes `SNI=api.cluster-x.example.com` to this cluster's `api-server:6443`.
3. `EP_ENDPOINTS` lists the edge virtual IPs, not the real `api-server:6443` addresses.
4. The `certificate-authority` in the kubeconfig is the CA that signed this cluster's API server certificate.

## Why discovery is disabled

In this topology `--enable-discovery=true` is actively harmful:

- The EndpointSlice for the `kubernetes` service in the `default` namespace lists the API server's **internal** advertised addresses. A remote worker reaches the control plane only through the edge, so those internal addresses are unreachable and would be injected into the load balancer pool as dead backends.
- With discovery enabled, extractedprism opens its own in-cluster client to `127.0.0.1:7445` with `ServerName=kubernetes.default.svc`. That SNI is not a routing key the shared edge knows about, so the edge cannot route it and discovery fails.

Static-only configuration avoids both problems.

## Why the whole path is DNS-free

This pattern preserves extractedprism's CNI-independence — it works before cluster networking is up, with no DNS lookups:

- kubelet does **not** resolve `api.cluster-x.example.com`. It connects to `127.0.0.1:7445` (the IP from `server`) and uses the name only as the SNI string and the certificate-verification name.
- extractedprism dials `198.51.100.10:443`, which is already an IP.

The correct SNI therefore appears on the wire without a single DNS lookup and without any SNI handling inside extractedprism, which stays a dumb passthrough.
