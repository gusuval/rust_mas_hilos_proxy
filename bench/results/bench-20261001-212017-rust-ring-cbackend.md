# Benchmark 2026-10-01 21:20

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
| `-t4 -c100 -d30s` | HTTPS | **201241** | 411.23us | 1.02ms | 0 |
| `-t4 -c200 -d30s` | HTTPS | **195717** | 689.12us | 1.82ms | 0 |
| `-t4 -c400 -d30s` | HTTPS | **163420** | 1.51ms | 4.55ms | 0 |
| `-t4 -c200 -d30s` | HTTP | **359452** | 515.77us | 1.53ms | 0 |

**Meta ≥ 50000 req/s HTTPS con 0 errores: alcanzada en todos los escenarios.**

Estadísticas del proxy al terminar:

```json
{
  "requests_total": 27642340,
  "responses": {
    "1xx": 0,
    "2xx": 27642340,
    "3xx": 0,
    "4xx": 0,
    "5xx": 0
  },
  "tls_handshakes": 700,
  "connections_accepted": 906,
  "upstream": {
    "connects": 802,
    "reuses": 27641538,
    "retries": 0
  }
}
```
