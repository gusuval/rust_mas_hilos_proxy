# 🚄 Proxy Inverso de Alto Rendimiento — Rust + hilos (mio: epoll/kqueue)

## 🎯 Objetivo del proyecto

Proxy inverso L7 asíncrono con terminación TLS, capaz de superar **50.000 peticiones/segundo sobre HTTPS** en Linux, portable entre Linux (`epoll`) y macOS (`kqueue`). Windows/IOCP queda fuera de alcance. La especificación está en [`SPEC.md`](SPEC.md) y el catálogo de requerimientos en [`docs/requerimientos.md`](docs/requerimientos.md).

Es el port a **Rust** de la versión en C11 con `pthread` ([`gusuval/version_pthread_proxy`](https://github.com/gusuval/version_pthread_proxy)): misma arquitectura (un hilo master y un hilo con su event loop por CPU), mismo comportamiento y la misma batería de integración, que pasa entera contra el binario de Rust. El historial de git conserva la versión en C.

Con este proyecto se aprende:

- **I/O no bloqueante y event loops** edge-triggered: cómo un solo hilo atiende miles de conexiones.
- `epoll` (Linux) y `kqueue` (macOS/BSD) tras una API común (`mio`).
- Multihilo con **`std::thread`** y **`SO_REUSEPORT`** (un hilo worker con su event loop por CPU); estado compartido con `Arc` y atómicos, estado del hilo con `Rc`/`Cell` y el sistema de tipos impidiendo mezclarlos.
- Balanceo round-robin/weighted/least-conn/least-load y health checks.
- Terminación **TLS** con **rustls** y selección de certificado por **SNI**.
- Build con **cargo**, tests unitarios con `cargo test` y benchmarking con **wrk**.

## 🏗️ Arquitectura

```
Client ──► [Frontend :8080 HTTP / :8443 TLS+SNI] ──► parse HTTP Host header ──► route lookup
        ──► [Backend pool] ──► estrategia (rr/weighted/least_conn/least_load)
        ──► conexión upstream (pool keep-alive, HTTP en claro)
        ──► respuesta ──► keep-alive / pipelining / túnel WebSocket
```

- **Un proceso: hilo master + N hilos worker** (`workers = "auto"` → nº de CPUs). El master valida la config, carga los certificados, vigila el fichero, atiende las señales, relanza un worker que termine (también por `panic`) y sirve las estadísticas. Cada worker tiene **un event loop** `mio` no bloqueante y edge-triggered (`EPOLLET` / `EV_CLEAR`) y un hilo de health checks; hay un único hilo de log para todo el proceso.
- **Config compartida, estado propio**: el master construye cada generación de configuración una sola vez (config validada, routers y `rustls::ServerConfig` con los certificados cargados) y la reparte a los workers en un `Arc`. Cada worker crea encima sus backends con estado local (pools keep-alive, contadores, balanceo) en `Rc`/`Cell`: el código del event loop no necesita locks, y el compilador impide que ese estado salga del hilo.
- Master → worker: un canal `mpsc` + `mio::Waker` por worker (config, parada). Worker → master: un guardia en el hilo que avisa al soltarse, también durante el unwinding de un `panic`.
- En Linux cada worker abre sus sockets con `SO_REUSEPORT` y el kernel reparte las conexiones. En macOS/BSD, donde `SO_REUSEPORT` no reparte carga, el master abre los sockets y los workers usan un duplicado.
- Configuración en **TOML** (frontends, rutas por dominio, backends con pesos) con **recarga en caliente automática** (inotify) **y vía SIGHUP**.

### Módulos

| Módulo | Responsabilidad |
|--------|-----------------|
| `master` | Lanzamiento, join y relanzamiento de hilos worker, señales (self-pipe), vigilancia del fichero (inotify), recarga, socket de estadísticas |
| `worker` | Cuerpo del hilo worker: event loop `mio`, listeners `SO_REUSEPORT`, swap de generaciones de config, tick de estadísticas, parada ordenada |
| `conn` | Máquina de estados cliente↔upstream: TLS, cabeceras, cuerpos en streaming, pool keep-alive y reintentos, pipelining, túnel WebSocket, timeouts |
| `runtime` | `Compiled` (generación compartida, inmutable) y `Runtime` (generación del worker) con liberación por refcount (RCU) |
| `tls` | Terminación TLS (rustls, proveedor `aws-lc-rs` o `ring`), resolvedor de certificado por SNI |
| `http` | Parser incremental HTTP/1.x sin copias (offsets sobre el buffer), protección frente a smuggling, cuerpos `Content-Length`/chunked |
| `router` | Hash djb2 para dominios exactos, wildcards `*.dom` (sufijo más largo), ruta default |
| `balancer` | round_robin / weighted / least_conn / least_load (`X-Backend-Load`) + salud pasiva |
| `health` | Sondas TCP/HTTP activas en hilo aparte |
| `config` | Parser TOML (crate `toml`) con validación estricta (rechaza claves desconocidas) |
| `log` | Cola MPSC acotada (4096 mensajes) con hilo consumidor; etiqueta por hilo |
| `buffer` | Pool de slots de 16 KB por hilo y buffer lineal |
| `timer` | Min-heap con reprogramación perezosa |
| `slab` | Slab con generaciones: tokens de mio/timers seguros frente a eventos tardíos |
| `stats` | Slot por hilo worker (atómicos + tabla de servidores bajo mutex) y JSON agregado |

Herramientas de prueba (`src/bin/`): `test_backend` (backend HTTP/1.1 + WebSocket sobre `mio`) y `ws_probe` (cliente WebSocket con TLS).

## ⚙️ Funcionalidades

- Múltiples frontends (puertos de escucha) y enrutamiento L7 por header `Host:`.
- Resolución de rutas: dominio exacto → wildcard `*.example.com` → `default` → `404` (sin backend sano → `503`).
- Frontends TLS con SNI (varios certificados por puerto, TLS 1.2 y 1.3).
- HTTP/1.1 completo: keep-alive cliente↔proxy y proxy↔backend, pipelining, cuerpos `Content-Length` y `chunked`, WebSocket (`Upgrade`).
- Balanceo `round_robin`, `weighted`, `least_conn` y `least_load` (según la cabecera `X-Backend-Load` que envían los backends) con exclusión de backends caídos.
- Health checks activos (sondas) y pasivos (fallos de conexión).
- **Recarga de configuración sin cortar conexiones**, automática al guardar el fichero o con SIGHUP.
- Endpoint de estadísticas JSON.
- Un `panic` en un worker solo tumba ese hilo: el master lo relanza.

## 💡 Solución

1. **Edge-triggered obliga a drenar**: con `EPOLLET`/`EV_CLEAR` el kernel avisa una sola vez por cambio; cada handler lee/escribe hasta `WouldBlock`. `sess_drive` repite leer → procesar → escribir mientras haya progreso.
2. **Reload sin downtime**: al detectar un cambio (inotify) o recibir SIGHUP (self-pipe trick), el master parsea, valida y carga los certificados; si todo va bien manda la nueva generación (`Arc`) a los workers, que hacen el swap. Las peticiones en vuelo guardan un `Rc` de la generación vieja y terminan con ella.
3. **Logging sin bloquear el event loop**: los mensajes van a una cola acotada y un hilo aparte los escribe; si se llena, se descarta y se cuenta.
4. **Cuatro buffers de 16 KB por conexión**: los cuerpos se reenvían en streaming, con contrapresión natural. En TLS, el buffer de rustls se limita a 64 KB por conexión.
5. **Tokens con generación**: sesiones, upstreams y listeners viven en slabs; el token de mio lleva índice + generación, así que un evento o timer de un objeto ya cerrado no alcanza al que reutiliza su hueco. La liberación se aplaza al final de cada iteración, como en la versión C.
6. **Sin `unsafe` en el camino de datos**: solo se usa en las llamadas al sistema del master (señales, inotify).

Las decisiones de diseño, con sus alternativas, están en [`docs/decisiones.md`](docs/decisiones.md). La trazabilidad requerimiento → test está en [`docs/verificacion.md`](docs/verificacion.md).

**Documentación completa** (versión Rust, con las comparativas de todas las ejecuciones de benchmark):
- [Documentación técnica (PDF)](docs/pdf/documentacion-tecnica.pdf): arquitectura, módulos, protocolos, pruebas, rendimiento y comparativas fork/pthread/Rust.
- [Manual de usuario (PDF)](docs/pdf/manual-de-usuario.pdf): instalación, configuración, operación, resolución de problemas y rendimiento.
- [Presentación (PPTX)](docs/presentacion/proxy-l7.pptx): evolución del proyecto, decisiones técnicas, benchmark, comparativas y coste.
- [Resumen de la primera sesión (versión C)](docs/Resumen.md).

Los fuentes de los PDF están en `docs/pdf/src/` (HTML + CSS de impresión, renderizados con Chrome headless: `google-chrome --headless --no-pdf-header-footer --print-to-pdf=…`) y la presentación se genera con `docs/presentacion/generar.mjs` (pptxgenjs).

## 📊 Benchmark (Linux, wrk, build release)

Meta: **≥ 50.000 req/s sobre HTTPS** (TLS 1.3, keep-alive), 30 s, 0 errores (`SPEC.md` §11). Resultado con `bench/bench_proxy.sh`, todo en Rust (proxy y `test_backend`), dos ejecuciones:

| Configuración | Protocolo | Req/s | Latencia media | p99 | Errores |
|---|---|---|---|---|---|
| `-t4 -c100 -d30s` | HTTPS | **201.168** / **202.156** | 401 / 407 us | 1,01 / 1,08 ms | 0 |
| `-t4 -c200 -d30s` | HTTPS | **193.383** / **189.936** | 716 / 720 us | 2,02 / 2,10 ms | 0 |
| `-t4 -c400 -d30s` | HTTPS | **163.089** / **163.825** | 1,55 / 1,53 ms | 5,04 / 5,46 ms | 0 |
| `-t4 -c200 -d30s` | HTTP (informativo) | **335.182** / **341.340** | 561 / 556 us | 1,68 / 1,76 ms | 0 |

Meta **superada en los tres escenarios HTTPS** (entre 3,2× y 4×), con 0 errores.

| Entorno | |
|---|---|
| CPU | AMD Ryzen 7 7735HS (8 núcleos / 16 hilos), WSL2 |
| Kernel | Linux 6.18 (WSL2) |
| Proxy | 8 workers, release + LTO (`codegen-units = 1`), rustls 0.23 + aws-lc-rs, access log desactivado |
| Backends | 2 × `test_backend` (2 hilos cada uno), respuesta de 100 B |
| Afinidad (`taskset`) | wrk: CPUs 0-3 · proxy: 4-11 · backends: 12-15 |

Informes: [`bench-20261001-213629-rust.md`](bench/results/bench-20261001-213629-rust.md), [`bench-20261001-213831-rust.md`](bench/results/bench-20261001-213831-rust.md).

**Rust frente a C** (misma máquina y sesión, con el mismo backend en C para aislar el proxy; [comparativa](bench/results/comparativa-rust-c-20261001.md)):

| Escenario | C + OpenSSL | Rust + rustls/ring | Rust + rustls/aws-lc |
|---|---|---|---|
| HTTPS `-c100` | 228.532 | 201.241 | 203.985 |
| HTTPS `-c200` | 220.261 | 195.717 | 197.585 |
| HTTPS `-c400` | 177.788 | 163.420 | 162.959 |
| HTTP `-c200` | 348.992 | 359.452 | 365.279 |

En HTTP el proxy en Rust iguala o supera al de C. En HTTPS queda un 8-12 % por debajo: con respuestas de 100 B pesa el coste por registro de rustls (buffers internos y copias), no el cifrado (cambiar `ring` por `aws-lc-rs` no lo mueve).

## 🚀 Cómo ejecutar

Requisitos: Rust estable con edición 2024 (probado con 1.98) y, para el proveedor TLS por defecto (`aws-lc-rs`), un compilador de C y `cmake`. Sin ellos: `--no-default-features -F ring`. Para los tests de integración y el benchmark: curl, jq, socat, openssl y wrk. En Ubuntu:

```bash
sudo apt-get install build-essential cmake curl jq socat openssl wrk
```

```bash
# Build (target/debug) y build optimizado (target/release)
cargo build
cargo build --release

# Tests unitarios (43)
cargo test

# Tests de integración (103 comprobaciones en debug: genera config y
# certificados, lanza 12 backends y el proxy, y limpia al terminar)
./tests/integration/run.sh                  # binarios de target/debug
./tests/integration/run.sh target/release   # 99: sin la prueba de pánico (solo debug)
cargo test -- --ignored                     # lo mismo desde cargo

# Demo con certificados de prueba
./tests/gen_config.sh demo
./target/release/test_backend --port 19001 --name api-1 &
./target/release/proxy -c demo/proxy.toml
curl -H 'Host: api.test' http://127.0.0.1:18080/
curl --cacert demo/certs/ca.pem --resolve api.test:18443:127.0.0.1 https://api.test:18443/

# Opcional para la demo: dar de alta los dominios en /etc/hosts
sudo ./tests/hosts.sh add

# Validar una config sin arrancar / ejecutar
./target/release/proxy -t -c proxy.toml
./target/release/proxy -c proxy.toml

# Recargar configuración: basta con guardar proxy.toml, o bien
kill -HUP <pid-proxy>

# Estadísticas
nc -U /tmp/proxy.sock

# Benchmark (compila en release, informe en bench/results/).
# PROXY_BIN / BACKEND_BIN permiten medir otros binarios (p. ej. los de C).
./bench/bench_proxy.sh
```

La referencia comentada de todas las opciones está en [`proxy.toml`](proxy.toml).

## ✅ Estado

- **Linux**: todo implementado y verificado: 43 tests unitarios, 103 comprobaciones de integración (99 en release) y el benchmark. `cargo clippy` sin avisos.
- **macOS/BSD (kqueue)**: `mio` usa kqueue y el código específico (sockets del master, sondeo del fichero de config en lugar de inotify) está escrito, pero no se ha compilado ni ejecutado en un Mac: hay que pasar los tests allí antes de darlo por bueno.
- **Windows/IOCP**: fuera de alcance.

## 📄 Licencia

[MIT](LICENSE).

<!-- BEGIN cc:que-se-valora -->
¡Hola! ¡Qué bueno que estés trabajando en tu proyecto "Proxy Epoll Kqueue"! Sé que es un reto, pero estoy aquí para ayudarte a entender qué es lo que buscamos cuando lo revisamos. Para que te quede claro, he preparado esta sección para el `README` de tu proyecto:

---

## 📋 Qué se valora

Cuando revisemos tu proyecto, nos fijaremos en varias cosas para entender qué tan bien lo has resuelto.

Primero, **lo que más pesa** es que tu proxy funcione como se espera y cumpla con todo lo que pide el enunciado. Queremos ver que hace lo que tiene que hacer, sin fallos y de forma robusta.

También le damos un **peso importante** a la calidad de tu código y a la arquitectura que has elegido. Nos interesa que tu código sea claro, fácil de entender y que la estructura general de tu proyecto tenga sentido y esté bien pensada.

El **vídeo demo** también tiene un **peso importante**. Es tu oportunidad para mostrarnos cómo funciona tu proxy en acción y explicarnos de forma concisa lo que has hecho.

Finalmente, aunque con un **peso menor**, valoramos la documentación que incluyas y las decisiones que hayas tomado. Nos ayuda a entender tu proceso de pensamiento y por qué hiciste las cosas de cierta manera.

Recuerda que el detalle del enunciado es lo que manda para saber qué se espera de tu proyecto, y la evaluación no penaliza por lo que el enunciado no pide explícitamente.

---
<!-- END cc:que-se-valora -->
