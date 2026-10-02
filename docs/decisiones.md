# Decisiones de diseño

Cada decisión con la alternativa descartada y el motivo.

## Port a Rust

**Rust con `mio` y hilos propios, no tokio.**
Alternativa: tokio (runtime asíncrono con *work stealing*). El enunciado pide programar contra epoll/kqueue y el diseño de la versión C (un event loop edge-triggered por hilo, sin compartir conexiones entre hilos) ya es el que mejor escala para un proxy. `mio` es una capa fina sobre epoll/kqueue: los mismos eventos, el mismo modo edge-triggered, sin un planificador encima. Así el port es una traducción fiel de la máquina de estados de C, y la comparación de rendimiento mide el lenguaje y la librería TLS, no dos arquitecturas distintas.

**rustls en lugar de OpenSSL.**
Alternativa: el crate `openssl` (el mismo OpenSSL 3.5 de la versión C). rustls está escrito en Rust (sin `unsafe` propio en el protocolo), solo implementa TLS 1.2/1.3 con cifrados modernos, y su API sin E/S encaja con un event loop: se le dan bytes cifrados (`read_tls`) y se le piden (`write_tls`), sin callbacks de BIO. A cambio, en HTTPS rinde un 8-12 % menos que OpenSSL con respuestas pequeñas ([comparativa](../bench/results/comparativa-rust-c-20261001.md)). El proveedor criptográfico (`aws-lc-rs` o `ring`) se elige en la compilación; no cambia el resultado.

**Config compartida (`Arc`) y estado del worker (`Rc`/`Cell`).**
La generación de configuración que construye el master (`Compiled`: config validada, routers, `rustls::ServerConfig` con los certificados) es inmutable y se comparte con `Arc`. Lo que cambia en cada petición (pools keep-alive, conexiones activas, carga, índices de round robin) vive en el `Runtime` de cada worker con `Rc` y `Cell`, que no son `Send`: el compilador impide que ese estado se use desde otro hilo por error. Lo único compartido de verdad entre hilos es la salud de cada servidor (`Arc<ServerShared>` con atómicos: la tocan el worker y su hilo de sondas) y las estadísticas.

**Slab con generaciones en lugar de punteros.**
Sesiones, upstreams y listeners viven en slabs. El token que se registra en mio (y en los timers) lleva clase, índice y generación: un evento o timer de un objeto ya cerrado, cuyo hueco se ha reutilizado, no casa con la generación nueva y se ignora. Es la versión segura de "marcar `closed` e ignorar eventos tardíos" de C.

**Un `panic` en un worker solo termina ese hilo, y el master lo relanza.**
El hilo del worker lleva un guardia que, al soltarse (también durante el *unwinding* de un `panic`), marca el slot de estadísticas y avisa al master por un canal y su `Waker`. El master hace `join` y relanza el worker con el mismo backoff que la versión C con procesos. Con el perfil `panic = "unwind"` esto recupera RF-03, que la versión C con hilos había perdido. Lo comprueba la batería de integración en los builds de depuración, donde `SIGUSR1` provoca un `panic` en un worker. Un fallo de memoria seguiría tumbando el proceso, pero el código del proxy no usa `unsafe` fuera de las llamadas al sistema del master (señales, inotify).

## Hilos y event loop

**Un proceso con un hilo master + N hilos worker, un event loop por hilo.**
Alternativa: master + N procesos worker con `fork()` (la primera versión en C). Hay un solo proceso que gestionar (un PID, un `kill -HUP`), un único hilo de log y menos memoria. El código del event loop es monohilo: cada worker tiene sus sesiones, upstreams, timers y pool de buffers.

- Master → worker: un canal `mpsc` y un `mio::Waker` por worker, por los que van la configuración y la parada. El master construye el `Poll` del worker y le envía la configuración inicial antes de lanzar el hilo, así que el worker la encuentra al arrancar.
- Las señales solo las atiende el hilo master (self-pipe: el handler escribe el número de señal en un pipe que vigila el `Poll` del master). Los workers se crean con todas las señales bloqueadas (`pthread_sigmask`), y sus hilos de health heredan esa máscara.
- Estadísticas: cada worker escribe en su slot (alineado a 64 B, contra el *false sharing*). Los contadores son atómicos y la tabla de servidores se publica cada 250 ms bajo un `Mutex` por slot.

**`SO_REUSEPORT` por worker en Linux; sockets del master en macOS/BSD.**
En Linux el kernel reparte las conexiones entre los sockets del grupo, sin *thundering herd*. En macOS `SO_REUSEPORT` permite el bind, pero no reparte: entrega las conexiones a un único socket. Allí el master abre los sockets, los incluye en la generación de configuración y cada worker usa un duplicado (`try_clone`), también en las recargas que añaden frontends.

**Edge-triggered, cada socket registrado una sola vez con lectura+escritura.**
Alternativa: level-triggered, o edge-triggered activando la escritura solo cuando hace falta. Registrar una vez y llevar dos flags por socket (`rd`/`wr`: "el último intento no dio `WouldBlock`") evita un `epoll_ctl(MOD)` por cambio de estado. `sess_drive()` repite leer → procesar → escribir mientras haya progreso. Así nunca queda trabajo pendiente sin un evento que lo despierte, que es el error típico con ET. Una sesión que agota su presupuesto (32 vueltas) se replanifica al final de la iteración para no acaparar el loop.

**Liberación diferida.**
Una sesión cerrada puede tener más eventos en el mismo lote de `epoll_wait`, y otras funciones de la misma pila de llamadas aún la referencian por índice. Al cerrarla se quita del `Poll` y se marca `closed`; el hueco de la slab se libera al final de la iteración. Los eventos tardíos no casan por la generación del token.

**Timers en min-heap con actualización perezosa.**
Cada byte recibido "refresca" un timeout. Retrasar un deadline solo actualiza un campo (O(1)); el timer se reubica al vencer. Solo adelantarlo reordena el heap. Sin un fd por timer.

**Reloj cacheado por iteración, actualizado antes de despachar.**
Un fallo real encontrado por los tests de la versión C: si el reloj se actualiza después de despachar los eventos, un handler que llega tras un periodo ocioso calcula deadlines ya vencidos, y aparecen 408/504 espurios. `Worker::run` lo actualiza justo después de `poll` y antes de despachar.

## HTTP

**La cabecera se parsea cuando está completa; el cuerpo lo delimita una máquina de estados aparte.**
Alternativa: un parser byte a byte con estado para todo (tipo `http-parser` de Node). Buscar `\r\n\r\n`, recordando hasta dónde se ha escaneado, tolera cualquier fragmentación sin reescanear. Después la cabecera se parsea de una vez, con punteros al buffer y sin copias. El cuerpo (`Content-Length`, chunked con extensiones y trailers, o delimitado por cierre) solo se sigue para saber dónde termina: los bytes se reenvían tal cual.

**Cuatro buffers de 16 KB por sesión y copia en espacio de usuario.**
Alternativa: `splice()`/zero-copy. Con TLS los datos pasan por espacio de usuario de todos modos y las cabeceras hay que reescribirlas. Los buffers salen de un pool de slots de 16 KB por hilo y se devuelven en cuanto la conexión queda ociosa: una conexión keep-alive inactiva no retiene buffers. La cabecera máxima es de 16 KB (un slot); si la cabecera reescrita no cabe, se responde 431.

**Pipelining en serie.**
Las peticiones pipelined se leen por adelantado (hasta llenar el buffer), pero se despachan de una en una: la siguiente se despacha cuando la anterior se ha terminado de recibir del backend. El orden de las respuestas queda garantizado sin colas y cada petición puede ir a un backend distinto.

**Errores locales con `Connection: close`.**
Las respuestas generadas por el proxy (400, 404, 408, 421, 431, 502, 503, 504) cierran la conexión. Tras un error puede quedar un cuerpo a medio leer, y cerrar es lo seguro. El cierre es "con linger": se vacía la salida, `shutdown(SHUT_WR)` y se descarta lo que siga llegando durante 2 s, para que un RST no se coma la respuesta.

**Reintento solo en un caso muy acotado.**
Una conexión del pool puede estar muerta sin que el proxy lo sepa todavía. Se reintenta una vez, en una conexión nueva, solo si la conexión era reutilizada, no llegó ni un byte de respuesta y la petición es idempotente y sin cuerpo. Para eso se conserva la cabecera enviada en el buffer. Un POST nunca se reintenta: el backend podría haberlo procesado.

**Protección frente a request smuggling.**
Se rechazan `Content-Length` + `Transfer-Encoding`, varios `Content-Length` distintos, TE sin `chunked` final, obs-fold y espacios antes de `:`. Las cabeceras nombradas en `Connection:` se eliminan, excepto `Host`, `Content-Length` y `Transfer-Encoding` (si no, se podría forzar al proxy a quitar el encuadre).

## Balanceo y salud

**`least_load`: EMA + power of two choices.**
La carga llega en `X-Backend-Load` con cada respuesta, se suaviza con una media móvil (α = 0,3) y caduca a los 5 s. Elegir siempre el mínimo haría que todos los workers se lanzaran a la vez sobre el mismo servidor entre dos actualizaciones. Con P2C (dos candidatos al azar, gana el de menos carga) se reparte mejor y sigue favoreciendo al menos cargado. Si la carga es desconocida o hay empate, decide `least_conn`. Con empate también en conexiones, gana el servidor sin carga fresca: así se conoce su carga cuanto antes. Con el desempate aleatorio inicial, un test de integración resultó inestable (35/40 en vez de ≥ 38).

**Estado de salud por worker.**
Cada worker tiene su hilo de sondas, sin compartir estado con los demás. Es más simple y no hay locks entre workers, a cambio de multiplicar las sondas por el número de workers. Las estadísticas muestran `up_workers` y un estado `degraded` si los workers discrepan.

**Recuperación pasiva half-open.**
Sin health activo, un servidor caído vuelve a probarse pasado `interval`. Si falla otra vez, cae con un solo fallo, sin esperar otros `fall`.

## Configuración y recarga

**El master construye la generación una vez y la comparte, en vez de que cada worker relea el fichero.**
Así todos los workers aplican exactamente la versión que validó el master, aunque el fichero cambie mientras tanto, y los certificados se leen una sola vez (en la versión C cada worker los releía). El master descarta además los eventos del watcher que no cambian el contenido.

**Generaciones con refcount (RCU sin atómicos).**
El `Runtime` de un worker agrupa la generación compartida, los backends con su estado y el hilo de health. Cada petición guarda un `Rc` al empezar y lo suelta al terminar; el swap es cambiar el `Rc` del worker. Todo pasa en el hilo del loop, así que el refcount no necesita atómicos. Una generación retirada se libera cuando solo queda la referencia de la lista de retiradas (se comprueba en el tick de 250 ms); antes se cierran las conexiones ociosas de su pool. Su hilo de health recibe por `Arc` lo que usa, así que no hay que esperarlo: se le pide que pare y termina por su cuenta.

**inotify sobre el directorio, no sobre el fichero.**
vim y `sed -i` guardan escribiendo un fichero nuevo y renombrándolo encima. Un watch sobre el fichero se perdería; uno sobre el directorio, filtrando por nombre, cubre la escritura in situ y el rename. Fuera de Linux se sondea la fecha y el tamaño del fichero cada 500 ms (no probado en macOS).

**Validación estricta.**
Las claves desconocidas son un error (`workerz = 2` no se ignora en silencio). Una config inválida al arrancar impide el arranque; en una recarga se descarta y se mantiene la anterior.

## TLS

**Un resolvedor de certificado por frontend (`ResolvesServerCert`).**
Es el mecanismo de rustls: el resolvedor recibe el `ClientHello` y devuelve el certificado. SNI exacto, luego el wildcard de una sola etiqueta más largo (RFC 6125) y, si no, `default_cert`. La comprobación 421 (SNI ≠ Host) usa el patrón `sni` de la config, no los SAN del certificado. TLS 1.3 y 1.2, con preferencia de cifrados del servidor. El texto plano pendiente de cifrar se limita a 64 KB por conexión para conservar la contrapresión.

## Limitaciones conocidas

- Un cliente HTTP/1.0 recibe tal cual una respuesta chunked del backend; no se convierte a longitud fija.
- Cambiar `workers` requiere reiniciar (se avisa en el log).
- En HTTPS con respuestas pequeñas, rustls rinde un 8-12 % menos que OpenSSL (ver la comparativa). La API *unbuffered* de rustls podría recortar la diferencia.
- `upstream_connect` no se prueba automáticamente: en loopback un connect rechazado falla al instante y no hay timeout.
- macOS/BSD: no se ha compilado ni ejecutado en este entorno (ver el README).
