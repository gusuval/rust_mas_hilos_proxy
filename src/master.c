#include "master.h"

/*
 * Hilo master: valida la config, lanza N hilos worker (cada uno con su
 * event loop), atiende las señales del proceso, vigila el fichero de
 * config, reparte las recargas y sirve las estadísticas.
 *
 * Los workers son hilos del mismo proceso (pthread), no procesos hijos:
 * comparten memoria, pero cada uno tiene su estado (_Thread_local) y solo
 * hablan con el master por su canal (socketpair). Al ser un solo proceso,
 * un fallo grave (SIGSEGV) en un worker termina todo el proxy; lo que sí se
 * relanza es un worker cuyo hilo termina por su cuenta (p. ej. config
 * rechazada al arrancar).
 */
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "io_event.h"
#include "listener.h"
#include "log.h"
#include "stats.h"
#include "tls.h"
#include "util.h"
#include "watch.h"
#include "worker.h"

#define RELOAD_DEBOUNCE_MS 200
#define RESPAWN_DELAY_MS 500
#define SHUTDOWN_GRACE_MS 12000

typedef struct {
    io_handler_t h;
    int fd;
} fdh_t;

typedef struct {
    int id;
    pthread_t thr;
    bool running;   /* hilo lanzado y aún sin join */
    fdh_t chan;     /* extremo del master; EOF = el hilo ha terminado */
    int worker_fd;  /* extremo del worker (lo cierra el propio hilo) */
    uint64_t started;
    int quick_crashes;
    bool pending_respawn;
} wslot_t;

typedef struct {
    char key[128];
    int fd;
} mlisten_t;

static struct {
    io_loop_t *loop;
    char cfg_path[CFG_PATH_MAX];
    char base_dir[CFG_PATH_MAX];
    config_t *cfg;
    char *text;
    int nworkers;
    wslot_t *w;
    stats_shm_t *stats;
    bool stopping;
    bool forced_exit;
    uint64_t stop_deadline;
    int sig_pipe[2];
    fdh_t sig_h, watch_h, stats_h;
    watch_t *watch;
    io_timer_t debounce, respawn, stop_timer;
    char stats_path[CFG_PATH_MAX];
    mlisten_t ml[CHAN_MAX_FDS]; /* sockets propios (macOS/BSD) */
    int nml;
} M;

/* ------------------------------------------------------------------ */

static void on_signal(int sig)
{
    int saved = errno;
    unsigned char c = (unsigned char)sig;
    ssize_t r = write(M.sig_pipe[1], &c, 1);
    (void)r;
    errno = saved;
}

/* Sockets de escucha del master (solo sin SO_REUSEPORT con reparto). */
static int sync_master_listeners(const config_t *cfg, char *err, size_t errlen)
{
#if PROXY_PER_WORKER_LISTEN
    (void)cfg;
    (void)err;
    (void)errlen;
    return 0;
#else
    mlisten_t next[CHAN_MAX_FDS];
    int nn = 0;
    if (cfg->nfrontends > CHAN_MAX_FDS) {
        snprintf(err, errlen, "máximo %d frontends", CHAN_MAX_FDS);
        return -1;
    }
    for (int i = 0; i < cfg->nfrontends; i++) {
        const char *key = cfg->frontends[i].key;
        int fd = -1;
        for (int k = 0; k < M.nml; k++)
            if (M.ml[k].fd >= 0 && strcmp(M.ml[k].key, key) == 0) {
                fd = M.ml[k].fd;
                M.ml[k].fd = -1;
            }
        if (fd < 0) {
            fd = listener_open(key, false, err, errlen);
            if (fd < 0) {
                for (int k = 0; k < nn; k++) /* devolver los ya movidos */
                    M.ml[M.nml++] = next[k];
                return -1;
            }
        }
        str_copy(next[nn].key, key, sizeof(next[nn].key));
        next[nn++].fd = fd;
    }
    for (int k = 0; k < M.nml; k++)
        if (M.ml[k].fd >= 0)
            close(M.ml[k].fd);
    memcpy(M.ml, next, sizeof(next[0]) * (size_t)nn);
    M.nml = nn;
    return 0;
#endif
}

static int send_config(int i)
{
    size_t bl = strlen(M.base_dir), tl = strlen(M.text);
    size_t kl = 0;
    for (int k = 0; k < M.nml; k++)
        kl += strlen(M.ml[k].key) + 1;
    size_t len = bl + 1 + tl + 1 + kl;
    char *p = malloc(len + 1);
    if (!p)
        return -1;
    memcpy(p, M.base_dir, bl + 1);
    memcpy(p + bl + 1, M.text, tl + 1);
    char *q = p + bl + 1 + tl + 1;
    int fds[CHAN_MAX_FDS];
    for (int k = 0; k < M.nml; k++) {
        size_t l = strlen(M.ml[k].key);
        memcpy(q, M.ml[k].key, l);
        q[l] = '\n';
        q += l + 1;
        fds[k] = M.ml[k].fd;
    }
    int r = chan_send(M.w[i].chan.fd, CHAN_CONFIG, p, (uint32_t)len, fds, M.nml);
    free(p);
    return r;
}

static void *worker_thread(void *arg)
{
    wslot_t *w = arg;
    char name[16];
    snprintf(name, sizeof(name), "proxy-w%d", w->id);
#if defined(__linux__)
    pthread_setname_np(pthread_self(), name);
#elif defined(__APPLE__)
    pthread_setname_np(name);
#endif
    int rc = worker_main(w->id, w->worker_fd, &M.stats->w[w->id]);
    /* Cerrar nuestro extremo despierta al master (EOF en su extremo). */
    close(w->worker_fd);
    return (void *)(intptr_t)rc;
}

static void chan_event(io_handler_t *h, uint32_t ev);

static int spawn(int i)
{
    wslot_t *w = &M.w[i];
    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
        LOGE("socketpair: %s", strerror(errno));
        return -1;
    }
    set_cloexec(sv[0]);
    set_cloexec(sv[1]);
    w->id = i;
    w->chan.fd = sv[0];
    w->chan.h.on_event = chan_event;
    w->worker_fd = sv[1];
    /* La config va antes de lanzar el hilo: la encuentra al arrancar. */
    if (send_config(i) < 0) {
        LOGE("no se pudo enviar la config al worker %d", i);
        close(sv[0]);
        close(sv[1]);
        return -1;
    }
    /* El hilo hereda la máscara: todas las señales bloqueadas, así solo
     * las recibe el hilo master. */
    sigset_t all, old;
    sigfillset(&all);
    pthread_sigmask(SIG_SETMASK, &all, &old);
    int err = pthread_create(&w->thr, NULL, worker_thread, w);
    pthread_sigmask(SIG_SETMASK, &old, NULL);
    if (err) {
        LOGE("pthread_create: %s", strerror(err));
        close(sv[0]);
        close(sv[1]);
        return -1;
    }
    w->running = true;
    w->started = mono_ms();
    w->pending_respawn = false;
    io_loop_add(M.loop, sv[0], IO_READ, &w->chan.h);
    return 0;
}

static void reload(bool forced)
{
    char *text = read_file(M.cfg_path, NULL);
    if (!text) {
        LOGE("recarga: no se puede leer %s: %s", M.cfg_path, strerror(errno));
        atomic_fetch_add(&M.stats->reloads_failed, 1);
        return;
    }
    if (!forced && M.text && strcmp(text, M.text) == 0) {
        free(text);
        return; /* sin cambios reales */
    }
    char err[512];
    config_t *cfg = config_parse(text, M.base_dir, err, sizeof(err));
    if (cfg && tls_validate_config(cfg, err, sizeof(err)) < 0) {
        config_free(cfg);
        cfg = NULL;
    }
    if (cfg && sync_master_listeners(cfg, err, sizeof(err)) < 0) {
        config_free(cfg);
        cfg = NULL;
    }
    if (!cfg) {
        LOGE("recarga rechazada, se mantiene la configuración actual: %s", err);
        atomic_fetch_add(&M.stats->reloads_failed, 1);
        free(text);
        return;
    }
    if (cfg->workers != M.nworkers)
        LOGW("el cambio de 'workers' (%d -> %d) requiere reiniciar el proxy",
             M.nworkers, cfg->workers);
    free(M.text);
    M.text = text;
    config_free(M.cfg);
    M.cfg = cfg;
    log_set_level(cfg->log_level);
    for (int i = 0; i < M.nworkers; i++)
        if (M.w[i].running && send_config(i) < 0)
            LOGE("no se pudo enviar la config al worker %d", i);
    atomic_fetch_add(&M.stats->reloads_ok, 1);
    LOGI("configuración recargada (%s)", forced ? "SIGHUP" : "cambio en fichero");
}

static void debounce_fire(io_timer_t *t)
{
    (void)t;
    if (!M.stopping)
        reload(false);
}

static void respawn_fire(io_timer_t *t)
{
    (void)t;
    if (M.stopping)
        return;
    for (int i = 0; i < M.nworkers; i++)
        if (M.w[i].pending_respawn) {
            if (spawn(i) == 0) {
                atomic_fetch_add(&M.stats->worker_restarts, 1);
                LOGI("worker %d relanzado", i);
            } else {
                io_timer_after(M.loop, &M.respawn, RESPAWN_DELAY_MS);
            }
        }
}

/* El hilo del worker i ha terminado (EOF en su canal): join y relanzar. */
static void worker_exited(int i)
{
    wslot_t *w = &M.w[i];
    void *ret = NULL;
    io_loop_del(M.loop, w->chan.fd);
    close(w->chan.fd);
    w->chan.fd = -1;
    pthread_join(w->thr, &ret);
    w->running = false;
    if (M.stopping)
        return;
    LOGE("worker %d terminó con código %d", i, (int)(intptr_t)ret);
    uint64_t alive = mono_ms() - w->started;
    w->quick_crashes = alive < 1000 ? w->quick_crashes + 1 : 0;
    if (w->quick_crashes > 10) {
        LOGE("worker %d falla al arrancar repetidamente; se abandona", i);
        return;
    }
    w->pending_respawn = true;
    io_timer_after(M.loop, &M.respawn,
                   w->quick_crashes ? RESPAWN_DELAY_MS * (uint64_t)w->quick_crashes : 50);
}

static void chan_event(io_handler_t *h, uint32_t ev)
{
    (void)ev;
    wslot_t *w = CONTAINER_OF(h, wslot_t, chan.h);
    char c;
    ssize_t n;
    /* El worker nunca escribe: solo interesa el EOF. */
    while ((n = recv(w->chan.fd, &c, 1, MSG_DONTWAIT)) > 0)
        ;
    if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR))
        worker_exited(w->id);
}

static int alive_workers(void)
{
    int n = 0;
    for (int i = 0; i < M.nworkers; i++)
        if (M.w[i].running)
            n++;
    return n;
}

static void stop_check(io_timer_t *t)
{
    if (alive_workers() == 0) {
        io_loop_stop(M.loop);
        return;
    }
    if (mono_ms() >= M.stop_deadline) {
        /* Un hilo no se puede matar por separado: se sale del proceso con
         * los workers restantes aún vivos. */
        LOGW("%d workers no terminaron a tiempo; se fuerza la salida", alive_workers());
        M.forced_exit = true;
        io_loop_stop(M.loop);
        return;
    }
    io_timer_after(M.loop, t, 50);
}

static void begin_shutdown(int sig)
{
    if (M.stopping)
        return;
    M.stopping = true;
    M.stop_deadline = mono_ms() + SHUTDOWN_GRACE_MS;
    LOGI("señal %d: parando workers", sig);
    for (int i = 0; i < M.nworkers; i++)
        if (M.w[i].running && chan_send(M.w[i].chan.fd, CHAN_STOP, NULL, 0, NULL, 0) < 0)
            LOGE("no se pudo pedir la parada al worker %d", i);
    io_timer_after(M.loop, &M.stop_timer, 20);
}

static void sig_event(io_handler_t *h, uint32_t ev)
{
    (void)h;
    (void)ev;
    unsigned char c;
    while (read(M.sig_pipe[0], &c, 1) == 1) {
        switch (c) {
        case SIGHUP:
            if (!M.stopping)
                reload(true);
            break;
        case SIGTERM:
        case SIGINT:
        case SIGQUIT:
            begin_shutdown(c);
            break;
        }
    }
}

static void watch_event(io_handler_t *h, uint32_t ev)
{
    (void)h;
    (void)ev;
    if (watch_consume(M.watch))
        io_timer_after(M.loop, &M.debounce, RELOAD_DEBOUNCE_MS);
}

static void stats_event(io_handler_t *h, uint32_t ev)
{
    (void)h;
    (void)ev;
    for (;;) {
        int fd = accept(M.stats_h.fd, NULL, NULL);
        if (fd < 0)
            break;
        size_t len;
        char *json = stats_json(M.stats, &len);
        if (json) {
            write_all(fd, json, len);
            free(json);
        }
        close(fd);
    }
}

static int open_stats_socket(const char *path)
{
    if (!path[0])
        return -1;
    struct sockaddr_un sun;
    memset(&sun, 0, sizeof(sun));
    sun.sun_family = AF_UNIX;
    if (strlen(path) >= sizeof(sun.sun_path)) {
        LOGE("stats_socket demasiado largo");
        return -1;
    }
    str_copy(sun.sun_path, path, sizeof(sun.sun_path));
    unlink(path);
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    if (bind(fd, (struct sockaddr *)&sun, sizeof(sun)) < 0 || listen(fd, 64) < 0) {
        LOGE("stats_socket %s: %s", path, strerror(errno));
        close(fd);
        return -1;
    }
    set_nonblock(fd);
    set_cloexec(fd);
    str_copy(M.stats_path, path, sizeof(M.stats_path));
    return fd;
}

int master_run(const char *cfg_path, config_t *cfg, char *cfg_text)
{
    memset(&M, 0, sizeof(M));
    str_copy(M.cfg_path, cfg_path, sizeof(M.cfg_path));
    str_copy(M.base_dir, cfg_path, sizeof(M.base_dir));
    char *slash = strrchr(M.base_dir, '/');
    if (slash)
        *slash = '\0';
    else
        str_copy(M.base_dir, ".", sizeof(M.base_dir));
    M.cfg = cfg;
    M.text = cfg_text;
    M.nworkers = cfg->workers;
    M.stats_h.fd = -1;

    char err[512];
#if PROXY_PER_WORKER_LISTEN
    for (int i = 0; i < cfg->nfrontends; i++)
        if (listener_test_bind(cfg->frontends[i].key, err, sizeof(err)) < 0) {
            LOGE("frontend '%s': %s", cfg->frontends[i].name, err);
            return 1;
        }
#else
    if (sync_master_listeners(cfg, err, sizeof(err)) < 0) {
        LOGE("%s", err);
        return 1;
    }
#endif

    M.stats = stats_create(M.nworkers);
    M.w = calloc((size_t)M.nworkers, sizeof(wslot_t));
    M.loop = io_loop_create(64);
    if (!M.stats || !M.w || !M.loop || pipe(M.sig_pipe) < 0) {
        LOGE("no se puede inicializar el master");
        return 1;
    }
    for (int i = 0; i < M.nworkers; i++)
        M.w[i].chan.fd = M.w[i].worker_fd = -1;
    set_nonblock(M.sig_pipe[0]);
    set_nonblock(M.sig_pipe[1]);
    set_cloexec(M.sig_pipe[0]);
    set_cloexec(M.sig_pipe[1]);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    sigaction(SIGHUP, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGQUIT, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    M.sig_h.h.on_event = sig_event;
    M.sig_h.fd = M.sig_pipe[0];
    io_loop_add(M.loop, M.sig_pipe[0], IO_READ, &M.sig_h.h);

    M.watch = watch_open(cfg_path);
    if (M.watch) {
        M.watch_h.h.on_event = watch_event;
        M.watch_h.fd = watch_fd(M.watch);
        io_loop_add(M.loop, M.watch_h.fd, IO_READ, &M.watch_h.h);
    } else {
        LOGW("no se puede vigilar %s: solo recarga por SIGHUP", cfg_path);
    }
    M.stats_h.fd = open_stats_socket(cfg->stats_socket);
    if (M.stats_h.fd >= 0) {
        M.stats_h.h.on_event = stats_event;
        io_loop_add(M.loop, M.stats_h.fd, IO_READ, &M.stats_h.h);
    }
    io_timer_init(&M.debounce, debounce_fire);
    io_timer_init(&M.respawn, respawn_fire);
    io_timer_init(&M.stop_timer, stop_check);

    LOGI("proxy pid %d, %d hilos worker, config %s", (int)getpid(), M.nworkers, cfg_path);
    for (int i = 0; i < M.nworkers; i++)
        if (spawn(i) < 0)
            return 1;

    io_loop_run(M.loop);

    if (M.stats_path[0])
        unlink(M.stats_path);
    if (M.forced_exit) {
        /* Quedan hilos usando stats y config: no se libera nada. */
        LOGI("master terminado (salida forzada)");
        return 0;
    }
    watch_close(M.watch);
    for (int k = 0; k < M.nml; k++)
        close(M.ml[k].fd);
    LOGI("master terminado");
    config_free(M.cfg);
    free(M.text);
    free(M.w);
    io_loop_destroy(M.loop);
    stats_destroy(M.stats);
    return 0;
}
