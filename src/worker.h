#ifndef WORKER_H
#define WORKER_H

#include <stdbool.h>
#include <stdint.h>
#include <sys/socket.h>

#include "buffer_pool.h"
#include "io_event.h"
#include "listener.h"
#include "runtime.h"
#include "stats.h"

struct session;

/* Estado de un worker: uno por hilo (_Thread_local), con su event loop. */
typedef struct worker {
    int id;
    io_loop_t *loop;
    bufpool_t *bp;
    stats_worker_t *st;
    runtime_t *rt;      /* generación actual */
    runtime_t *retired; /* pendientes de liberar */
    uint64_t gen;
    listener_t *listeners;
    struct session *sessions;
    int nsessions;
    bool stopping;
    uint64_t stop_deadline;
} worker_t;

extern _Thread_local worker_t W;

void rt_ref(runtime_t *rt);
void rt_unref(runtime_t *rt);

/* Cuerpo del hilo worker; vuelve tras CHAN_STOP o EOF del canal.
 * No cierra chan_fd (lo hace quien lanzó el hilo). */
int worker_main(int id, int chan_fd, stats_worker_t *st);

/* connection.c */
void session_accept(int fd, const struct sockaddr *peer, const char *fe_key);
void sessions_close_idle(void);
void sessions_close_all(void);
/* Libera la arena de sesiones del hilo (tras sessions_close_all). */
void sessions_free_arena(void);
void upstream_pool_drain(server_t *srv);

#endif
