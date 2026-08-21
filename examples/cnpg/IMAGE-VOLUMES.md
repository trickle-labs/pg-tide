# CloudNativePG Image Volume Extensions for pg_tide

> **Status:** Available from pg_tide v0.37.0 with CloudNativePG 1.28+

## Overview

[CloudNativePG Image Volume Extensions](https://cloudnative-pg.io/docs/1.28/imagevolume_extensions/) enable decoupling PostgreSQL extensions from the base PostgreSQL container image. Instead of embedding extensions at build time, you mount extension OCI images as read-only volumes at pod startup.

This guide shows how to use this pattern with pg_tide, offering several advantages over the [sidecar pattern](cluster.yaml):

- **Immutable base images** — Use official, minimal CloudNativePG PostgreSQL images
- **Simplified supply chain** — No custom PostgreSQL image builds; only distribute extension images
- **Easier updates** — Update extensions without rebuilding PostgreSQL base images
- **Better security** — Smaller attack surface; read-only extension mounts

## Requirements

- **CloudNativePG** v1.28 or later
- **Kubernetes** v1.35+ (or 1.33/1.34 with `ImageVolume` feature gate enabled)
- **PostgreSQL** 18+ (Image Volume Extensions require `extension_control_path` GUC)
- **Container runtime** with ImageVolume support:
  - `containerd` v2.1.0+
  - `CRI-O` v1.31+

## Step 1: Build the pg_tide Extension Image

The extension image must follow the standard OCI layout:

```
/share/extension/
  - pg_tide.control
  - pg_tide--0.51.0.sql (and all upgrade scripts)
/lib/
  - pg_tide.so
```

Build with the provided `Dockerfile.extension`:

```bash
cd /path/to/pg-tide
docker build \
  -f examples/cnpg/Dockerfile.extension \
  --build-arg PG_VERSION=18 \
  -t ghcr.io/your-org/pg-tide-extension:18-0.37.0 .

docker push ghcr.io/your-org/pg-tide-extension:18-0.37.0
```

### Multi-version images (optional)

To build a single image supporting multiple PostgreSQL versions, use multi-stage builds:

```dockerfile
FROM builder AS final-18
COPY --from=builder:18 /build/target/release/pg_tide-pg18/pg_tide.so /lib/

FROM builder AS final-19
COPY --from=builder:19 /build/target/release/pg_tide-pg19/pg_tide.so /lib/
```

Then tag and push separate images per PostgreSQL major version.

## Step 2: Deploy the Cluster

Use the Image Volume Extensions pattern with the `Cluster` resource:

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: pg-tide-cluster
spec:
  instances: 3
  imageName: ghcr.io/cloudnative-pg/postgresql:18

  postgresql:
    # CloudNativePG automatically configures:
    #   extension_control_path: /extensions/pg-tide/share
    #   dynamic_library_path: /extensions/pg-tide/lib
    parameters:
      shared_preload_libraries: ""  # pg_tide doesn't require preload

  # Mount pg_tide as an image volume extension
  extensions:
    - name: pg-tide
      image:
        reference: ghcr.io/your-org/pg-tide-extension:18-0.37.0

  bootstrap:
    initdb:
      database: app
      owner: app
      postInitSQL:
        - CREATE EXTENSION IF NOT EXISTS pg_tide;
        - CREATE ROLE relay_user LOGIN PASSWORD 'change-me';
        - GRANT USAGE ON SCHEMA tide TO relay_user;
```

See [examples/cnpg/cluster-image-volume.yaml](cluster-image-volume.yaml) for a complete example.

## Step 3: Verify Extension Installation

Once the cluster is running, verify pg_tide is available:

```bash
kubectl exec -it pg-tide-cluster-1 -c postgres -- psql -U app app

app=# CREATE EXTENSION pg_tide;
CREATE EXTENSION

app=# \dx
                     List of installed extensions
   Name   | Version |   Schema   |         Description
----------+---------+------------+-------------------------------
 pg_tide  | 0.37.0  | tide       | Transactional outbox + inbox
 plpgsql  | 1.0     | pg_catalog | PL/pgSQL procedural language
(2 rows)
```

## Advanced Topics

### Custom Paths (Non-standard Image Layouts)

If your extension image uses non-standard paths, override the defaults:

```yaml
extensions:
  - name: pg-tide
    extension_control_path:
      - custom/share/path
    dynamic_library_path:
      - custom/lib/path
    image:
      reference: ghcr.io/your-org/pg-tide-extension:18-0.37.0
```

### System Libraries

If your extension image bundles system libraries (e.g., for complex dependencies), make them available via `ld_library_path`:

```yaml
extensions:
  - name: pg-tide
    ld_library_path:
      - system/lib
    image:
      reference: ghcr.io/your-org/pg-tide-extension:18-0.37.0
```

**Note:** Changes to `ld_library_path` require a manual cluster restart:

```bash
kubectl cnpg restart pg-tide-cluster
```

### Multiple Extensions in One Image

If bundling multiple extensions, explicitly configure paths for each:

```yaml
extensions:
  - name: pg-tide
    extension_control_path:
      - pg-tide/share
    dynamic_library_path:
      - pg-tide/lib
    image:
      reference: ghcr.io/your-org/pg-extensions:18-multi
  - name: other-ext
    extension_control_path:
      - other-ext/share
    dynamic_library_path:
      - other-ext/lib
    image:
      reference: ghcr.io/your-org/pg-extensions:18-multi
```

## Updating Extensions

### Adding a new extension

1. Build and push the extension image
2. Add it to `.spec.postgresql.extensions` in your `Cluster` resource
3. CloudNativePG will trigger a rolling update to mount the new volume

**Important:** Test thoroughly in staging before updating production clusters, as pod restarts are required.

### Upgrading an extension

1. Build a new extension image with the updated version
2. Push it with a new tag (e.g., `:18-0.38.0`)
3. Update the image reference in `.spec.postgresql.extensions`
4. Update the version in the `Database` resource if using declarative database management

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Database
metadata:
  name: pg-tide-app
spec:
  name: app
  owner: app
  cluster:
    name: pg-tide-cluster
  extensions:
    - name: pg_tide
      version: "0.38.0"
```

## Comparison: Sidecar vs. Image Volume Extensions

| Aspect | Sidecar Pattern | Image Volume Extensions |
|--------|-----------------|------------------------|
| **Base image** | Custom (includes extension) | Official (minimal) |
| **Build complexity** | Custom Dockerfile + push | Extension Dockerfile |
| **Extension updates** | Rebuild base image | Update image reference |
| **Security** | Larger image, more attack surface | Minimal base image, read-only mounts |
| **CNPG version** | Any | 1.28+ |
| **Kubernetes version** | Any | 1.35+ (or 1.33/1.34 with feature gate) |
| **PostgreSQL version** | Any | 18+ (requires `extension_control_path`) |

## Troubleshooting

### Extension not found

If `CREATE EXTENSION pg_tide` fails:

```
ERROR:  could not open extension control file
```

Verify:
1. The extension image reference is correct
2. The image exists in your registry
3. The image follows the standard layout (`/share/extension/`, `/lib/`)
4. The pod has successfully mounted the volume:

```bash
kubectl exec pg-tide-cluster-1 -c postgres -- ls -la /extensions/pg-tide/
```

### Permission denied errors

If you see permission errors loading the `.so` file:

```
ERROR:  could not load library
```

Check file permissions in the image:

```bash
docker run -it ghcr.io/your-org/pg-tide-extension:18-0.37.0 bash
# Inside the container:
ls -la /lib/pg_tide.so
chmod +x /lib/pg_tide.so
```

### Pod restart loops

If pods restart after adding an extension, check the PostgreSQL logs:

```bash
kubectl logs pg-tide-cluster-1 -c postgres --tail=50
```

Review the Dockerfile for missing dependencies or incompatible PostgreSQL versions.

## See Also

- [CloudNativePG Image Volume Extensions documentation](https://cloudnative-pg.io/docs/1.28/imagevolume_extensions/)
- [Example Cluster resource](cluster-image-volume.yaml)
- [Extension Dockerfile](Dockerfile.extension)
- [Sidecar pattern example](cluster.yaml)
