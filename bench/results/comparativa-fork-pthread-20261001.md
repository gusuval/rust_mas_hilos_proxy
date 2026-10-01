# Comparativa: workers con `fork` vs. workers con `pthread`

Mismo script (`bench/bench_proxy.sh`), misma máquina y mismo día (2026-10-01), ejecutadas de forma alterna: fork → pthread → fork → pthread. La versión `fork` es el commit `16a7a6c` (`main`), compilada desde cero en un worktree aparte.

| Parámetro | Valor |
|---|---|
| CPU | AMD Ryzen 7 7735HS (8 núcleos / 16 hilos), WSL2 |
| Kernel / OpenSSL | Linux 6.18.35.2-microsoft-standard-WSL2 / 3.5.5 |
| Build | release + LTO |
| Proxy | 8 workers (procesos o hilos), access log desactivado |
| Backends | 2 × `test_backend` (2 hilos), respuesta de 100 B |
| Afinidad | wrk: CPUs 0-3 · proxy: 4-11 · backends: 12-15 |
| Carga | `wrk -t4`, keep-alive, 30 s por escenario |

## Req/s (2 ejecuciones por versión)

| Escenario | fork #1 | fork #2 | pthread #1 | pthread #2 |
|---|---|---|---|---|
| HTTPS `-c100` | 218.432 | 228.312 | 224.999 | 228.962 |
| HTTPS `-c200` | 200.599 | 220.085 | 219.959 | 206.081 |
| HTTPS `-c400` | 26.663 ⚠️ | 173.506 | 168.219 | 174.086 |
| HTTP `-c200` | 297.652 | 317.289 | 336.447 | 338.965 |

## Latencia p99

| Escenario | fork #1 | fork #2 | pthread #1 | pthread #2 |
|---|---|---|---|---|
| HTTPS `-c100` | 1,93 ms | 1,31 ms | 1,13 ms | 1,32 ms |
| HTTPS `-c200` | 3,10 ms | 2,06 ms | 1,83 ms | 4,13 ms |
| HTTPS `-c400` | 48,96 ms ⚠️ | 4,58 ms | 6,00 ms | 4,72 ms |
| HTTP `-c200` | 2,43 ms | 2,31 ms | 1,75 ms | 1,48 ms |

Errores: 0 en todas las ejecuciones salvo fork #1 con `-c400`.

⚠️ **fork #1, `-c400`**: wrk tardó 2,51 min en lugar de 30 s y acabó con 400 timeouts (uno por conexión). Durante la prueba iba a ~33,6k req/s por hilo de wrk (~134k en total); las req/s del informe se calculan sobre los 2,51 min. No se reprodujo en la segunda ejecución, así que no hay causa identificada. Se deja anotado, sin descartar el dato.

## Conclusiones

- **HTTPS**: rendimiento equivalente. Las diferencias entre versiones (±5 %) son del mismo tamaño que las que hay entre dos ejecuciones de la misma versión. El coste dominante es TLS, y cada worker sigue siendo un event loop independiente con su `SO_REUSEPORT`, tanto en procesos como en hilos.
- **HTTP**: la versión con hilos sale un 6-13 % por encima en las dos ejecuciones (336-339k frente a 298-317k req/s). La mejora es plausible, porque los hilos comparten espacio de direcciones (menos presión de TLB/caché que 8 procesos), pero con dos muestras no se puede asegurar.
- La meta (≥ 50.000 req/s HTTPS con 0 errores) se cumple con holgura en todos los escenarios de las dos ejecuciones de la versión con hilos.

Informes completos: `bench-20261001-202723-fork.md`, `bench-20261001-204108-fork.md`, `bench-20261001-203855-pthread.md` y `bench-20261001-204310-pthread.md`.
