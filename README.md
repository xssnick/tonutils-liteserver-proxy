# tonutils-liteserver-proxy

`tonutils-liteserver-proxy` is a TON liteserver proxy. It accepts liteserver client connections and forwards requests to one or more backend liteservers.

It can:

- balance requests across backend liteservers;
- rate-limit traffic by client IP and by client key;
- expose Prometheus metrics;
- cache blocks, accounts, libraries, and some responses;
- emulate some lightweight liteserver requests;
- route old block requests to archive liteservers.

When `DisableEmulationAndCache` is enabled, the proxy works as a simple passthrough: rate limiting, connection limiting, balancing, and archive routing only. All caches and emulation are disabled.

## Build

```bash
make proxy
```

The binary is written to:

```bash
build/liteserver
```

## First Run

If the config file does not exist, the proxy creates it automatically:

```bash
./build/liteserver -config ls-proxy-config.json
```

The generated config is saved with `0600` permissions. On startup, the proxy logs the public key for each configured client key:

```text
liteserver initialized pub_key=...
```

Use that public key in clients that connect to this proxy as a liteserver.

## Flags

- `-config`: path to the JSON config file. Default: `ls-proxy-config.json`.
- `-verbosity`: log level. `3` debug, `2` info, `1` warn, `0` error. Default: `2`.

## Example Config

JSON `[]byte` fields are encoded as base64 strings. This applies to client `PrivateKey` and backend liteserver `Key`.

```json
{
  "ListenAddr": "0.0.0.0:7445",
  "MetricsAddr": "0.0.0.0:8058",
  "MetricsNamespace": "mainnet",
  "DisableEmulationAndCache": false,
  "CacheConfig": {
    "DisableGetMethodsEmulation": false,
    "MaxCachedAccountsPerBlock": 512,
    "MaxCachedLibraries": 32768,
    "MaxMasterBlockSeqnoDiffToCache": 128,
    "MaxShardBlockSeqnoDiffToCache": 128
  },
  "Clients": [
    {
      "Name": "default",
      "PrivateKey": "BASE64_ED25519_SEED_32_BYTES",
      "CapacityPerIP": 100,
      "CapacityPerKey": 0,
      "CoolingPerSec": 20
    }
  ],
  "Backends": [
    {
      "Name": "fast-1",
      "Addr": "1.2.3.4:19949",
      "Key": "BASE64_BACKEND_LITESERVER_PUBLIC_KEY",
      "Role": "fast"
    },
    {
      "Name": "archive-1",
      "Addr": "5.6.7.8:19949",
      "Key": "BASE64_ARCHIVE_LITESERVER_PUBLIC_KEY",
      "Role": "archive"
    }
  ],
  "MaxConnectionsPerIP": 50,
  "MaxKeepAliveSeconds": 60,
  "ResponseGeneralCacheSize": 16384,
  "BalancerType": "fail_over"
}
```

## Config Fields

`ListenAddr`: TCP address where the proxy accepts liteserver clients.

`MetricsAddr`: HTTP address for Prometheus metrics. Metrics are exposed at `/metrics`.

`MetricsNamespace`: namespace for Prometheus metrics.

`DisableEmulationAndCache`: fully disables caches and emulation. Proxying, rate limits, connection limits, balancing, and archive routing still work.

`CacheConfig.DisableGetMethodsEmulation`: disables get-method emulation while keeping other caches enabled.

`CacheConfig.MaxCachedAccountsPerBlock`: account-state cache size per cached block.

`CacheConfig.MaxCachedLibraries`: global library cache size.

`CacheConfig.MaxMasterBlockSeqnoDiffToCache`: how far behind the latest master block a master block can be and still be cached.

`CacheConfig.MaxShardBlockSeqnoDiffToCache`: how far behind the latest known shard block a shard block can be and still be cached.

`Clients`: liteserver identities exposed by the proxy and their limits. `PrivateKey` is a base64-encoded Ed25519 seed. `CapacityPerIP`, `CapacityPerKey`, and `CoolingPerSec` control rate limiting.

`Backends`: backend liteservers. Empty `Role` is treated as `fast`. Valid roles are `fast` and `archive`.

`MaxConnectionsPerIP`: maximum active connections from a single IP address.

`MaxKeepAliveSeconds`: keep-alive limit and upper bound for wait-master request timeouts.

`ResponseGeneralCacheSize`: general-purpose response cache size when cache/emulation is enabled. Ignored when `DisableEmulationAndCache` is `true`.

`BalancerType`: `fail_over` or `round_robin`.

## Archive Routing

Archive routing is enabled by assigning backend roles:

```json
{
  "Backends": [
    { "Name": "fast-1", "Addr": "1.2.3.4:19949", "Key": "...", "Role": "fast" },
    { "Name": "archive-1", "Addr": "5.6.7.8:19949", "Key": "...", "Role": "archive" }
  ]
}
```

On startup, the proxy detects how much history each fast backend stores. It uses the archive backend to look up old master block IDs and checks whether those blocks are still available on fast backends. The result is used to compute `archiveThreshold`.

At request time, a request is routed to archive if it contains a block seqno and this condition is true:

```text
current_master_seqno - request_seqno >= archive_threshold
```

Requests without a block ID or seqno stay on fast backends. For example, `getTransactions` does not contain a block ID, so the proxy cannot safely determine whether the request is old and does not archive-route it.

## Proxy-Only Mode

To run without cache or emulation:

```json
{
  "DisableEmulationAndCache": true
}
```

In this mode:

- requests are proxied directly to backends;
- rate limits and connection limits still apply;
- archive routing still works for requests with block seqno;
- no general response cache is initialized;
- no block, account, or library cache is initialized;
- get-method emulation is not used.

Use this mode when responses must always reflect current backend liteserver state without any risk of serving stale cached data.

## Metrics

Prometheus metrics are exposed at:

```text
http://<MetricsAddr>/metrics
```

For the example config:

```bash
curl http://127.0.0.1:8058/metrics
```

## Development

Run tests:

```bash
go test ./...
```

Build the proxy:

```bash
make proxy
```
