# Verificación de requerimientos

Cada requerimiento de [`requerimientos.md`](requerimientos.md) con la
evidencia que lo verifica.

- **UT**: `cargo test` (43 tests, en el módulo `tests` de cada fichero de `src/`; se citan como `módulo::test`).
- **IT**: `tests/integration/run.sh` (103 comprobaciones con el build de depuración, 99 con el de release). Entre comillas va el nombre del caso tal como lo imprime el script.
- **BM**: `bench/bench_proxy.sh`.

Estado: ✅ verificado automáticamente · 🔶 verificado parcialmente o por revisión · ⬜ no verificable en este entorno.

## Funcionales

| ID | Evidencia | Estado |
|---|---|---|
| RF-01 | `mio` elige epoll/kqueue por plataforma; nada más en el código usa epoll/kqueue. UT `timer::order_cancel_lazy`, `slab::generations`. | ✅ Linux · ⬜ kqueue: no compilado en macOS |
| RF-02 | Revisión: todos los bucles de E/S (`conn.rs`, `worker.rs`, `test_backend.rs`) iteran hasta `WouldBlock`. IT "POST 10 MB …" (cuerpos mucho mayores que un buffer) | ✅ |
| RF-03 | IT "workers como hilos: sin procesos hijos", "un hilo proxy-wN por worker", "worker con pánico es relanzado", "tráfico normal tras relanzar el worker", "SIGTERM: parada ordenada de cada worker" (el pánico se provoca con `SIGUSR1`, solo en builds de depuración) | ✅ |
| RF-04 | Linux: `SO_REUSEPORT` por worker. IT "stats: agregado de 2 workers". macOS/BSD: el master abre los sockets y los workers usan un duplicado | ✅ Linux · ⬜ macOS |
| RF-05 | UT `timer::order_cancel_lazy`, `timer::rearm_after_fire`. Reloj actualizado antes de despachar: revisión de `Worker::run` | ✅ |
| RF-10 | IT: frontends `web`, `web-tls`, `admin` y `extra` (este último añadido en caliente) | ✅ |
| RF-11 | IT "TLS verificado contra la CA", "TLS 1.3 negociado", "TLS 1.2 aceptado" | ✅ |
| RF-12 | IT "SNI api.test -> cert api.test", "SNI a.example.com -> cert wildcard", "SNI desconocido -> default_cert", "sin SNI -> default_cert" | ✅ |
| RF-13 | IT "SNI distinto de Host no cubierto -> 421" y "... cubierto por el cert -> 200" | ✅ |
| RF-14 | Revisión: `connect_upstream()` usa sockets TCP en claro | ✅ |
| RF-20 | UT `router::many_hosts`. IT "dominio exacto …", "Host con puerto y mayúsculas" | ✅ |
| RF-21 | UT `router::exact_wildcard_default`, `tls::choose_by_sni`. IT "wildcard …", "ápex example.com -> default" | ✅ |
| RF-22 | IT "frontend sin default -> 404", "backend sin servidores sanos -> 503", "HTTP/1.1 sin Host -> 400" | ✅ |
| RF-23 | IT "cada petición keep-alive se enruta aparte" | ✅ |
| RF-30 | UT `http::fragmented_byte_by_byte`, `http::fragmented_random` | ✅ |
| RF-31 | UT `http::limits`, `http::malformed`. IT "cabecera > 16 KB -> 431", "petición malformada -> 400" | ✅ |
| RF-32 | IT "X-Forwarded-For concatenado", "X-Real-IP reemplazado", "X-Forwarded-Proto: http/https" | ✅ |
| RF-33 | UT `http::connection_tokens`. IT "hop-by-hop …", "cabecera listada en Connection eliminada" | ✅ |
| RF-34 | UT `http::smuggling`. IT "Content-Length + Transfer-Encoding -> 400" | ✅ |
| RF-35 | IT "POST 10 MB Content-Length íntegro" (SHA-256), "memoria acotada: pico RSS del proceso < workers × 64 MB" | ✅ |
| RF-36 | UT `http::body_chunked*`. IT "POST 10 MB chunked íntegro", "respuesta chunked", "chunked reenviado con trailers" | ✅ |
| RF-37 | IT "HEAD conserva Content-Length sin cuerpo", "204 sin cuerpo", "respuesta delimitada por cierre", "HEAD/204 + petición siguiente" | ✅ |
| RF-38 | IT "10 peticiones en 1 conexión cliente", "HTTP/1.0 …" | ✅ |
| RF-39 | IT "30 peticiones reutilizan conexiones al backend", "stats cuentan reutilizaciones". BM: ~800 conexiones al backend para ~27 M peticiones | ✅ |
| RF-40 | IT "reintento de conexión reutilizada que el backend descarta" (`test_backend --drop-every`), "conexiones del pool muertas no producen errores" | ✅ |
| RF-41 | IT "3 peticiones pipelined, 3 respuestas en orden", "pipelining con cuerpo intermedio" | ✅ |
| RF-42 | IT "upstream_read -> 504", "client_header incompleta -> 408". `upstream_connect`: por revisión (en loopback un connect rechazado falla al instante) | 🔶 |
| RF-43 | IT "WebSocket por HTTP", "WebSocket por TLS" (`ws_probe`), "Upgrade no aceptado -> respuesta normal" | ✅ |
| RF-50..53 | UT `balancer::round_robin`, `balancer::weighted_smooth`, `balancer::least_conn`. IT "round_robin 30 -> 10/10/10", "weighted 3:1 -> 30/10", "least_conn evita al servidor lento" | ✅ |
| RF-54 | UT `balancer::least_load`, `balancer::least_load_explores_unknown`. IT "least_load prefiere la carga baja" | ✅ |
| RF-55 | IT "X-Backend-Load eliminado hacia el cliente" | ✅ |
| RF-56 | UT `balancer::least_load_stale`, `balancer::load_values` | ✅ |
| RF-57 | UT `balancer::passive_health`. IT "backend caído: primer intento -> 502 … -> 503", "backend caído: 0 errores para el cliente" | ✅ |
| RF-58 | IT "health activo marca api-2 como down", "health activo recupera api-2 (rise)" | ✅ |
| RF-59 | UT `balancer::down_excluded` (las 4 estrategias), `balancer::exclude_mask` | ✅ |
| RF-60, 61 | UT `config::validation_rules` (19 reglas de validación, claves desconocidas), `config::full_example`, `config::minimal_defaults` | ✅ |
| RF-62 | IT "recarga automática (sed -i, rename) < 1 s", "recarga automática (escritura in situ)" | ✅ Linux · ⬜ macOS |
| RF-63 | IT "recarga manual con SIGHUP" | ✅ |
| RF-64 | IT "config inválida rechazada y contada", "… el tráfico sigue", "… error en el log" | ✅ |
| RF-65 | IT "petición en vuelo termina con la config vieja" | ✅ |
| RF-66 | IT "frontend añadido/eliminado en caliente", "certificados recargados (default_cert)" | ✅ |
| RF-70 | UT `log::queue_full_drops`, `log::queue_concurrent_producers` (4 productores × 10 000, FIFO por productor) | ✅ |
| RF-71 | IT "access log con campos" | ✅ |
| RF-72 | UT `stats::aggregates_workers`. IT "stats: JSON válido", "stats: campos principales" | ✅ |
| RF-80..84 | Los usa IT en cada ejecución; `hosts.sh`: `add` dos veces no duplica y `remove` deja el fichero idéntico | ✅ |

## No funcionales

| ID | Evidencia | Estado |
|---|---|---|
| RNF-01, 02 | BM: resultados en el README y en `bench/results/` | ✅ Linux |
| RNF-03 | IT "wrk + 8 recargas: sin errores de socket / sin respuestas no-2xx" | ✅ |
| RNF-04 | Revisión: DNS y certificados solo en el master al cargar la config; sondas en hilo aparte; log en hilo aparte | ✅ |
| RNF-05 | IT "memoria acotada …", "buffers liberados tras las subidas" | ✅ |
| RNF-06 | Revisión: `unsafe` solo en `master.rs` (sigaction, pthread_sigmask, inotify, errno en el handler de señales). El resto es Rust seguro: el compilador descarta UB y carreras de datos | ✅ |
| RNF-07 | RF-31 y RF-34 | ✅ |
| RNF-08 | Linux: compila y pasa todo. macOS/BSD: sin compilar ni ejecutar en este entorno | 🔶 |
| RNF-09 | `cargo build` y `cargo clippy --all-targets`: 0 avisos; `cargo fmt --check` limpio | ✅ |
| RNF-10 | `cargo build` desde cero (dependencias de crates.io; `aws-lc-rs` necesita cmake, o `-F ring` sin él) | ✅ |
