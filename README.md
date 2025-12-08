# check-metrics

`check-metrics` is a tiny helper that repeatedly scrapes Prometheus-style metrics from selected
Kubernetes pods, stores the raw snapshots on disk, and computes per-second rates for counters
without needing to wire up a full metrics stack. It is useful when debugging production incidents or
during controller development where you only need short-lived samples from a handful of pods.

## Features

- Scrapes pods defined via glob-style matchers and optional namespaces.
- Uses port-forwarding to avoid exposing metrics services externally.
- Stores every scrape under `~/.cache/check-metrics/<cluster>/<namespace>/<pod>/` using nanosecond
  timestamps.
- Locks per cluster so multiple scrapers do not fight over the cache.
- Computes per-second counter deltas locally via `check-metrics per-second <counter>`.

## Installation

You need Go 1.25+ (see `go.mod`). Fetch the binary with:

```bash
go install github.com/guettli/check-metrics@latest
```

Alternatively, clone this repository and run `go build` to produce a local binary.

`check-metrics` uses your normal Kubernetes config, so ensure `$KUBECONFIG` (or `~/.kube/config`)
points at the cluster you want to inspect.

## Configuration

The scraper looks for `.config/check-metrics/check-metrics.yaml`, starting in the current directory
and walking upward, unless `--config` is provided. The file describes which pods to scrape:

```yaml
defaultScrapeIntervalSeconds: 60   # optional, defaults to 30
pods:
  - name: "api-*"
    namespace: production
    port: 9090                      # optional; falls back to ports named "*metrics*" or 8080
  - name: "*-controller"
    namespace: staging
```

- `name` accepts shell-style globs (powered by `path.Match`).
- `namespace` defaults to all namespaces if not set.
- `port` is the container port that exposes `/metrics`. If omitted, the scraper tries to discover a
  port whose name contains `metrics`; otherwise it uses 8080.

## Usage

### Scrape

```bash
check-metrics scrape
```

This command:

1. Resolves the configuration file.
2. Builds a Kubernetes client using your current context, noting the cluster name to separate
   caches.
3. Discovers pods that match the configured patterns and are currently `Running`.
4. For each pod, establishes a temporary port-forward to `/metrics`, downloads the payload, and
   writes it to `~/.cache/check-metrics/<cluster>/<namespace>/<pod>/<timestamp>.prom`.

Scrapes repeat every `defaultScrapeIntervalSeconds` seconds (30s if unset). The process holds a
filesystem lock (`scrape.lock`) under the cluster cache directory so you do not accidentally launch
two scrapers against the same cluster.

### Per-second rate computation

```bash
check-metrics per-second http_requests_total
```

This command never contacts the cluster. It:

1. Locates the cache directory for your current kube context.
2. Finds the two newest snapshots for every namespace/pod combination.
3. Parses each snapshot, aggregates the named counter (labels are normalized and sorted), and
   computes the delta divided by the elapsed time between the snapshots.
4. Prints the rates sorted highest-first and notes how many series saw zero change.

Use this after a scrape has been running for a few intervals to quickly assess counter growth
without PromQL.

## Cache layout

```
~/.cache/check-metrics/
└── <cluster>/
    ├── scrape.lock
    └── <namespace>/<pod>/<timestamp>.prom
```

Timestamps are formatted as `20060102T150405.000000000Z` so lexicographic order matches chronology.
You can inspect the `.prom` files directly if needed.

## Development

Standard Go tooling works. To run or test:

```bash
go test ./...
go build ./...
```

## License

Apache License 2.0. See `LICENSE` for details.
