# Comparativa: proxy en Rust frente a proxy en C (ambos con hilos)

Mismo script (`bench/bench_proxy.sh`), misma máquina, mismo día (2026-10-01) y misma configuración: 8 workers, 2 backends de 2 hilos con respuestas de 100 B, `wrk -t4`, keep-alive, 30 s por escenario y CPUs fijadas (wrk 0-3, proxy 4-11, backends 12-15).

- **C**: versión `pthread` (C11 + epoll + OpenSSL 3.5.5), repositorio `version_pthread_proxy`, commit `8441edc`.
- **Rust**: este repositorio (mio + rustls 0.23, release + LTO, `codegen-units = 1`).

## Req/s

Para aislar el proxy, las columnas 1-3 usan el mismo backend (el `test_backend` en C). La columna 4 es el stack completo en Rust (proxy y backend), que es el resultado oficial del README.

| Escenario | 1. C + OpenSSL | 2. Rust + rustls/ring | 3. Rust + rustls/aws-lc | 4. Rust + aws-lc, backend Rust (2 ejecuciones) |
|---|---|---|---|---|
| HTTPS `-c100` | 228.532 | 201.241 | 203.985 | 201.168 / 202.156 |
| HTTPS `-c200` | 220.261 | 195.717 | 197.585 | 193.383 / 189.936 |
| HTTPS `-c400` | 177.788 | 163.420 | 162.959 | 163.089 / 163.825 |
| HTTP `-c200` | 348.992 | 359.452 | 365.279 | 335.182 / 341.340 |

Errores: 0 en todas las ejecuciones.

Mediciones previas de la versión C en la misma máquina (comparativa `fork`/`pthread`): HTTPS 225-229k / 206-220k / 168-174k y HTTP 336-339k. La columna 1 está dentro de ese rango.

## Latencia p99

| Escenario | 1. C | 3. Rust + aws-lc | 4. Rust, backend Rust |
|---|---|---|---|
| HTTPS `-c100` | 1,32 ms | 1,29 ms | 1,01 / 1,08 ms |
| HTTPS `-c200` | 2,20 ms | 1,93 ms | 2,02 / 2,10 ms |
| HTTPS `-c400` | 4,66 ms | 5,88 ms | 5,04 / 5,46 ms |
| HTTP `-c200` | 1,48 ms | 1,61 ms | 1,68 / 1,76 ms |

## Conclusiones

- **HTTP**: con el mismo backend, el proxy en Rust iguala o supera al de C (359-365k frente a 349k). La parte propia del proxy (event loop, parser, buffers, pool keep-alive) no es más lenta que en C.
- **HTTPS**: el proxy en Rust queda un 8-12 % por debajo. Como en HTTP no hay diferencia, la causa es la capa TLS. Cambiar el proveedor criptográfico de `ring` a `aws-lc-rs` no la mueve (columna 2 frente a 3), así que no es el cifrado AES-GCM. Es el coste por registro de rustls (buffers internos con memoria dinámica y una copia más del texto plano), que pesa mucho con respuestas de 100 B. Un allocator más rápido (`mimalloc`) solo dio un 2-3 %, dentro del ruido, y se descartó (sin informe guardado).
- Formas de cerrar la diferencia, no exploradas: la API *unbuffered* de rustls (cifra sobre los buffers propios, sin copias intermedias) o usar OpenSSL desde Rust (crate `openssl`).
- La meta de 50.000 req/s por HTTPS se supera entre 3,2× y 4× en todos los escenarios.
- **Backend de pruebas**: la primera versión del `test_backend` en Rust ponía a cero un buffer de 64 KB en cada evento, y eso limitaba el escenario HTTP a ~298k req/s (informe `bench-20261001-211808-rust-ring.md`). Con un buffer reutilizado por hilo llega a 335-341k.

## Informes

| Informe | Combinación |
|---|---|
| `bench-20261001-212221-c-pthread.md` | 1. C + OpenSSL, backend C |
| `bench-20261001-212017-rust-ring-cbackend.md` | 2. Rust + ring, backend C |
| `bench-20261001-212616-rust-awslc-cbackend.md` | 3. Rust + aws-lc, backend C |
| `bench-20261001-213629-rust.md`, `bench-20261001-213831-rust.md` | 4. Rust + aws-lc, backend Rust |
| `bench-20261001-211808-rust-ring.md` | Rust + ring, primera versión del backend Rust |
