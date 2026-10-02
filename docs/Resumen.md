# Resumen de las sesiones

Dos sesiones con Claude Code (Claude Opus 5.5), en hora local (UTC−3):

| Sesión | Fecha | Resultado | Coste a precios de API |
|---|---|---|---|
| 1 | 26-09-2026, 18:32–20:52 | Proxy en C11 con procesos (`fork`), de la spec al MR | ≈ $22,67 |
| 2 | 01-10-2026, 20:18–22:24 | Versión C con hilos (`pthread`), port a Rust, comparativas y documentación | ≈ $24,77 |

## Sesión 2 — De procesos a hilos, y de C a Rust

Punto de partida: el proyecto de la sesión 1 copiado en un directorio nuevo, todavía asociado al repo de GitLab. Resultado: dos repos nuevos en GitHub (la versión C con hilos y este, en Rust), las dos implementaciones verificadas con la misma batería de pruebas, 11 ejecuciones de benchmark comparadas y la documentación actualizada.

### Línea de tiempo

| Hora | Paso | Resultado |
|---|---|---|
| 20:18 | Asociar el directorio a un repo nuevo de GitHub y desasociarlo de GitLab | `gusuval/version_pthread_proxy` (público); remotos de GitLab y del repo antiguo eliminados; resultados de benchmark pendientes subidos |
| 20:25 | Cambiar los workers de `fork` a `pthread`, con tests y benchmark | Benchmark de referencia de la versión `fork`, implementación, 99/99 pruebas de integración en build normal, ASan+UBSan y ThreadSanitizer |
| ~20:40 | Benchmark alterno fork → pthread → fork → pthread | HTTPS equivalente; HTTP +6-13 % con hilos |
| 20:52 | Commit, merge a `main` y push; rama borrada | `638a257`, merge `8441edc` |
| 20:54 | Nuevo repo con el proyecto | `gusuval/rust_mas_hilos_proxy`, con todo el historial |
| 20:55 | Port completo a Rust con hilos | ~7.100 líneas: proxy, `test_backend` y `ws_probe`. 43 tests unitarios y 103/103 de integración a la primera |
| ~21:15 | Benchmarks Rust frente a C | 5 combinaciones (ring/aws-lc, backend C/Rust, mimalloc); diferencia aislada en la capa TLS |
| 22:07 | Commit, merge a `main` y push; rama borrada | `58361e0`, merge `d9bccf9` |
| 22:08 | PDF y presentación para la versión Rust, con las comparativas de todas las ejecuciones | Técnica (20 págs.), manual (17 págs.), PPTX de 13 diapositivas; commit `39d83c7` |
| 22:23 | Este resumen | |

### Decisiones

Del usuario: pasar los workers a hilos, crear un repo aparte para el port, portar a Rust **con hilos**, fusionar cada cambio en `main` con un commit de merge, y añadir las comparativas de las ejecuciones a los PDF y a la presentación.

Tomadas por Claude y justificadas en [`decisiones.md`](decisiones.md):

| Tema | Decisión |
|---|---|
| Visibilidad de los repos nuevos | Públicos, como el repo de GitHub que ya existía |
| Fin de un hilo worker (C) | El master lo detecta por el EOF de su `socketpair` (equivalente a `SIGCHLD`) |
| Estadísticas entre hilos (C) | Mutex por slot en lugar del seqlock (carrera de datos según ThreadSanitizer) |
| Event loop en Rust | `mio`, un loop por hilo, no tokio: port fiel y comparación justa con C |
| TLS en Rust | rustls (proveedor `aws-lc-rs` por defecto, `ring` como alternativa) en lugar de OpenSSL |
| Estado compartido | `Arc` para la config compilada, salud y estadísticas; `Rc`/`Cell` para el estado de cada worker |
| Herramientas de prueba | Portadas a Rust: el repo queda 100 % Rust y la batería de integración de C se reutiliza tal cual |
| Fallo de un worker (Rust) | Un `panic` solo termina su hilo y el master lo relanza; se prueba con `SIGUSR1` en builds de depuración |
| `PROMPT.md` (pedía C y Meson) | Se mantiene; la desviación queda anotada en la trazabilidad de `requerimientos.md` |

### Qué se construyó

| Parte | Contenido |
|---|---|
| Versión C con hilos | 18 ficheros cambiados (319 +, 258 −): estado `_Thread_local`, canal con `CHAN_STOP`, liberación completa al terminar un hilo, etiqueta de log por hilo |
| `src/` (Rust) | 15 módulos de la biblioteca + `main.rs`: master, worker, conn, http, tls, router, balancer, health, runtime, config, log, stats, buffer, timer, slab |
| `src/bin/` (Rust) | `test_backend` (HTTP/1.1 + WebSocket sobre `mio`) y `ws_probe` (cliente WebSocket con rustls) |
| `tests/` | `integration/run.sh` adaptado (pruebas de hilos y de relanzamiento tras un `panic`) y envoltorio `cargo test -- --ignored` |
| `bench/` | `bench_proxy.sh` con cargo y binarios intercambiables (`PROXY_BIN`, `BACKEND_BIN`); 2 comparativas en `bench/results/` |
| Documentación | README, `SPEC.md`, `decisiones.md`, `requerimientos.md`, `verificacion.md`, 2 PDF y presentación |

### Resultados

- **Versión C con hilos**: 5/5 suites unitarias y 99/99 comprobaciones de integración en build normal, ASan+UBSan (sin informes, también de fugas) y ThreadSanitizer (sin avisos).
- **Versión Rust**: 43 tests unitarios, 103/103 comprobaciones de integración en debug y 99/99 en release, `cargo clippy` sin avisos. `unsafe` solo en `master.rs` (11 bloques).
- **Benchmark** (req/s, medias; WSL2, Ryzen 7 7735HS, 8 workers, 30 s por escenario, 0 errores en todas las ejecuciones válidas):

  | Escenario | C, fork | C, pthread | Rust |
  |---|---|---|---|
  | HTTPS, 100 conexiones | 223.372 | 227.498 | 201.662 |
  | HTTPS, 200 conexiones | 210.342 | 215.434 | 191.660 |
  | HTTPS, 400 conexiones | 173.506 | 173.364 | 163.457 |
  | HTTP, 200 conexiones | 307.471 | 341.468 | 338.261 |

  Con el mismo backend, Rust iguala o supera a C en HTTP y queda un 8-12 % por debajo en HTTPS, por el coste por registro de rustls frente a OpenSSL. Todas las versiones superan la meta de 50k req/s por HTTPS más de 3 veces.

### Problemas encontrados

1. **Builds copiados de otro proyecto**: `build/`, `build-release/` y `build-asan/` compilaban los fuentes del repo antiguo. Las primeras pruebas pasaban contra el código viejo; se regeneraron los tres.
2. **`meson test` no recompila el proxy** que usa la batería de integración: hay que compilar antes. Queda anotado en la memoria del proyecto.
3. **Carrera de datos en el seqlock** de las estadísticas al pasar a hilos (ThreadSanitizer). Sustituido por un mutex por slot.
4. **Hilos de health con el nombre del worker**: heredaban `proxy-wN` y confundían la prueba de "un hilo por worker". Ahora se llaman `proxy-hwN`.
5. **Ejecución anómala de la versión `fork`** (400 conexiones): wrk terminó con 400 timeouts y 2,5 min en vez de 30 s. No se reprodujo; queda documentada sin causa.
6. **`test_backend` en Rust lento**: ponía a cero un buffer de 64 KB en cada evento y limitaba HTTP a ~298k req/s. Con un buffer reutilizado, 335-341k.
7. **Detalles de Rust 2024**: `gen` es palabra reservada (se renombró el campo).
8. **Maquetación**: etiquetas solapadas en la figura de arquitectura y un capítulo de comparativas mal paginado, detectados al revisar los PDF renderizados.

### Incidencias del entorno

- El conector MCP de GitHub no conectaba; se usó `gh` (ya autenticado).
- Sin `perf` no se pudo perfilar la diferencia de HTTPS; se aisló con mediciones (mismo backend, cambio de proveedor criptográfico, otro allocator).
- Sin `pdftoppm` para revisar los PDF: se usó `pypdfium2` en un entorno virtual del scratchpad. La presentación se revisó convirtiéndola a PDF con LibreOffice. Los PDF se generaron con Chrome headless.

### Coste

Tokens exactos sumados del transcript de la sesión (198 respuestas de `claude-opus-5-5`, deduplicadas por id), valorados a precios de la API de Claude Opus 5.5:

| Concepto | Tokens | Precio / M | USD |
|---|---|---|---|
| Lectura de caché | 67.123.632 | $0,20 | $13,42 |
| Salida | 317.686 | $20,00 | $6,35 |
| Escritura de caché (TTL 1 h) | 623.629 | $8,00 | $4,99 |
| Entrada sin caché | 396 | $4,00 | $0,00 |
| **Total** | | | **≈ $24,77** |

Medido hasta las 22:24, justo antes de escribir este resumen. La diapositiva de coste de la presentación usa el corte de las 22:09 (≈ $18,76), antes de la tarea de documentación. Intervención humana: 16 mensajes cortos, ninguno con código.

### Pendiente

- **Probar en macOS/BSD**: la versión Rust no se ha compilado allí.
- Recortar el ~10 % de HTTPS frente a C (API *unbuffered* de rustls u OpenSSL desde Rust), si hace falta.
- Vídeo demo (lo pide la rúbrica).

## Sesión 1 — Proxy en C11 con procesos

Sesión del 26-09-2026 (hora local, UTC−3) con Claude Code (Claude Opus 5.5). Punto de partida: un `README.md` y un `PROMPT.md` con requisitos breves. Resultado: un proxy inverso L7 en C11 implementado, probado, medido y enviado a revisión.

### Línea de tiempo

| Hora | Paso | Resultado |
|---|---|---|
| 18:32 | Refinar las specs de `PROMPT.md` y `README.md`, preguntando las dudas | 12 preguntas en 3 rondas. `SPEC.md` nuevo y README coherente |
| ~18:45 | Redactar los requerimientos en `docs/requerimientos.md` | RF-01…RF-84 y RNF-01…RNF-10, con criterios de aceptación y trazabilidad |
| ~19:10 | Implementar | Entorno sin compilador: el usuario instaló las dependencias. Código, tests y herramientas |
| ~19:40 | Pruebas y corrección de bugs | 93 → 96 comprobaciones de integración en verde, también con ASan/UBSan |
| ~19:45 | Benchmark | 172k–224k req/s HTTPS, 0 errores |
| 19:59 | Commit en la rama `feat/proxy-l7` | 4 commits temáticos |
| ~20:10 | Push y MR | Credenciales caducadas; tras autenticarse el usuario, [MR !1](https://gitlab.codecrypto.academy/uval.gustavo/1.5.10-proxy-epoll-kqueue/-/merge_requests/1) |
| ~20:15 | Documentación en PDF, presentación y este resumen | Técnica (17 págs.), manual (16 págs.), PPTX de 10 diapositivas |

### Decisiones tomadas con el usuario

| Tema | Decisión |
|---|---|
| Windows / IOCP | Fuera de alcance |
| Balanceo por carga | El backend envía `X-Backend-Load` en cada respuesta y el proxy la elimina antes de reenviarla al cliente |
| Recarga | Automática (vigilando el fichero) **y** por SIGHUP |
| TLS | Obligatorio, con SNI; hacia los backends, HTTP en claro |
| Keep-alive | En ambos lados (cliente y pool de conexiones a los backends) |
| HTTP | Content-Length, chunked, pipelining y WebSocket |
| Backend de pruebas | Sobre la misma abstracción `io_event` (kqueue y epoll) |
| Dominios en los tests | `curl --resolve`; script opcional para `/etc/hosts` |
| Benchmark | ≥ 50k req/s **sobre HTTPS**, solo en Linux |
| Tests de WebSocket | Sin python3-websockets (petición del usuario): cliente propio en C (`ws_probe`) |

Otras decisiones las tomó Claude y están justificadas en [`decisiones.md`](decisiones.md): errores locales con `Connection: close`, health por worker, reintento solo para peticiones idempotentes, pipelining en serie, etc.

### Qué se construyó

| Parte | Contenido | Líneas |
|---|---|---|
| `src/` | Proxy: 38 ficheros, 15 módulos (event loop, master/worker, HTTP, TLS, router, balanceo, health, config, recarga, log, estadísticas) | ~6.800 |
| `tools/` | `test_backend` (HTTP/1.1 + WebSocket) y `ws_probe` (cliente WebSocket, con TLS) | ~830 |
| `tests/` | 31 tests cmocka en 5 suites, script de integración con 96 comprobaciones, `gen_config.sh`, `hosts.sh` | ~1.500 |
| `bench/` | `bench_proxy.sh` (release + LTO, afinidad de CPU, informe) | ~170 |
| Documentación | `SPEC.md`, `requerimientos.md`, `decisiones.md`, `verificacion.md`, README, `proxy.toml` comentado | ~900 |

La dependencia tomlc99 (MIT) está vendorizada como subproject de Meson.

### Resultados

- **Tests**: 5/5 suites unitarias y 96/96 comprobaciones de integración, estables en ejecuciones repetidas. Con AddressSanitizer + UBSan + LeakSanitizer, 0 informes. 0 warnings con `-Wall -Wextra -Wpedantic`.
- **Benchmark** (WSL2, Ryzen 7 7735HS, 8 workers, 30 s por escenario):

  | Escenario | Req/s | p99 |
  |---|---|---|
  | HTTPS, 100 conexiones | 223.705 | 1,29 ms |
  | HTTPS, 200 conexiones | 213.565 | 2,37 ms |
  | HTTPS, 400 conexiones | 172.075 | 4,78 ms |
  | HTTP, 200 conexiones | 337.904 | 1,80 ms |

  28,5 M peticiones sin errores; la meta de 50k se supera entre 3,4× y 4,5×.

### Bugs encontrados por los tests

1. **Reloj del event loop atrasado** (real, grave): `io_loop_now()` se actualizaba después de despachar los eventos. Tras un periodo ocioso, los deadlines nacían vencidos y habría habido 408/504 espurios. Se corrigió en epoll y kqueue y tiene test de regresión.
2. **Desempate aleatorio en `least_load`**: un test salía inestable (35/40). Ahora se prefiere el servidor sin carga conocida, para conocerla cuanto antes.
3. **Dos bugs en `test_backend`**: al rebobinar el buffer de entrada se corrompían los cuerpos grandes.
4. **Tres errores del propio script de tests**: salto de línea eliminado por `$(...)`, recuento de peticiones propias a `/stats`, y `pgrep` que se encontraba a sí mismo.

### Incidencias del entorno

- No había compilador ni Meson; el usuario instaló los paquetes (sin sudo no se podía).
- El push y el MR fallaron por falta de credenciales (token de `glab` caducado, claves SSH no dadas de alta). Se resolvió con `glab auth login`.
- Para generar los PDF no había pandoc ni LaTeX. Se usó Chromium headless de Playwright, con sus librerías de sistema extraídas de paquetes `.deb` sin instalarlos.

### Coste

Tokens exactos sumados del transcript de la sesión (175 respuestas de `claude-opus-5-5`), valorados a precios de la API de Claude Opus 5.5:

| Concepto | Tokens | Precio / M | USD |
|---|---|---|---|
| Lectura de caché | 57.178.538 | $0,20 | $11,44 |
| Salida | 375.979 | $20,00 | $7,52 |
| Escritura de caché (TTL 1 h) | 464.065 | $8,00 | $3,71 |
| Entrada sin caché | 350 | $4,00 | $0,00 |
| **Total** | | | **≈ $22,67** |

El 99 % de la entrada se leyó de caché. Con plan Claude Max no se factura por token: la cifra es la referencia a precios de API. Está medida hasta la actualización de la presentación, así que no incluye los últimos mensajes de la sesión.

### Pendiente

- **Probar en macOS/BSD**: el código kqueue solo tiene comprobada la sintaxis.
- Revisar y fusionar el MR !1.
- Vídeo demo (lo pide la rúbrica).
