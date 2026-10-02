# Benchmark 2026-10-01 21:26

| Parámetro | Valor |
|---|---|
| CPU | AMD Ryzen 7 7735HS with Radeon Graphics (16 hilos) |
| Memoria | 7 GB |
| Kernel | Linux 6.18.35.2-microsoft-standard-WSL2 |
| OpenSSL | 3.5.5 |
| Build | release + LTO |
| Proxy | `target/release/proxy` |
| Backend | `/home/guval/missea/version_pthread_proxy/build-release/tools/test_backend` |
| Workers del proxy | 8 |
| Backends | 2 × test_backend (2 hilos) |
| Afinidad | wrk: CPUs 0-3, proxy: 4-11, backends: 12-15 |
| Petición | GET / (respuesta de 100 B), keep-alive, 30s |

| Configuración | Protocolo | Req/s | Latencia media | p99 | Errores |
|---|---|---|---|---|---|
| `-t4 -c100 -d30s` | HTTPS | **203985** | 412.06us | 1.29ms | 0 |
| `-t4 -c200 -d30s` | HTTPS | **197585** | 704.47us | 1.93ms | 0 |
| `-t4 -c400 -d30s` | HTTPS | **162959** | 1.57ms | 5.88ms | 0 |
| `-t4 -c200 -d30s` | HTTP | **365279** | 504.16us | 1.61ms | 0 |

**Meta ≥ 50000 req/s HTTPS con 0 errores: alcanzada en todos los escenarios.**

Estadísticas del proxy al terminar:

```json
{
  "requests_total": 27910079,
  "responses": {
    "1xx": 0,
    "2xx": 27910079,
    "3xx": 0,
    "4xx": 0,
    "5xx": 0
  },
  "tls_handshakes": 700,
  "connections_accepted": 906,
  "upstream": {
    "connects": 800,
    "reuses": 27909279,
    "retries": 0
  }
}
```
