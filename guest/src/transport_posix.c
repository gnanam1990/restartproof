/* POSIX transport: Unix-domain SOCK_STREAM for the worker channel and
 * TCP for the control channel. Only compiled off-QNX. */
#ifndef __QNXNTO__

#include "rp_transport.h"
#include "rp_common.h"

#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <arpa/inet.h>

struct rp_listener { int fd; char path[108]; };
struct rp_conn { int fd; };

static int wait_io(int fd, short events, int timeout_ms) {
    struct pollfd p = { fd, events, 0 };
    int r;
    do { r = poll(&p, 1, timeout_ms); } while (r < 0 && errno == EINTR);
    if (r <= 0) return r;             /* 0 timeout, <0 error */
    if (p.revents & events) return 1; /* data/status ready first */
    return (p.revents & (POLLERR | POLLHUP)) ? -2 : 1;
}

static int read_full(int fd, void *buf, size_t len) {
    char *p = buf;
    while (len) {
        ssize_t n = read(fd, p, len);
        if (n == 0) return 0;                       /* clean EOF */
        if (n < 0) { if (errno == EINTR) continue; return -1; }
        p += n; len -= (size_t)n;
    }
    return 1;
}

static int write_full(int fd, const void *buf, size_t len) {
    const char *p = buf;
    while (len) {
        ssize_t n = write(fd, p, len);
        if (n < 0) { if (errno == EINTR) continue; return -1; }
        p += n; len -= (size_t)n;
    }
    return 0;
}

/* ---- worker endpoint (server side) ---- */

int rp_listen(const char *endpoint, rp_listener_t **out) {
    rp_listener_t *l = calloc(1, sizeof *l);
    if (!l) return -1;
    l->fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (l->fd < 0) { free(l); return -1; }
    struct sockaddr_un a;
    memset(&a, 0, sizeof a);
    a.sun_family = AF_UNIX;
    if (strlen(endpoint) >= sizeof a.sun_path) { free(l); return -1; }
    strcpy(a.sun_path, endpoint);
    unlink(endpoint);
    if (bind(l->fd, (struct sockaddr *)&a, sizeof a) < 0 ||
        listen(l->fd, 4) < 0) { free(l); return -1; }
    strncpy(l->path, endpoint, sizeof l->path - 1);
    *out = l;
    return 0;
}

int rp_listen_recv(rp_listener_t *l, rp_msg_deliver_t *out, rp_conn_t **cli,
                   int timeout_ms) {
    /* returns 0 with *cli set, 1 on timeout, <0 on error — a timeout
     * must never look like a successful accept with a NULL cli */
    int r = wait_io(l->fd, POLLIN, timeout_ms);
    if (r == 0) return 1;
    if (r < 0) return r;
    int fd = accept(l->fd, NULL, NULL);
    if (fd < 0) return -1;
    int rr = read_full(fd, out, sizeof *out);
    if (rr <= 0) { close(fd); return rr - 2; }      /* -1 or -2 */
    rp_conn_t *c = calloc(1, sizeof *c);
    if (!c) { close(fd); return -1; }
    c->fd = fd;
    *cli = c;
    return 0;
}

int rp_conn_reply(rp_conn_t *c, const rp_msg_reply_t *msg) {
    return write_full(c->fd, msg, sizeof *msg);
}

void rp_conn_close(rp_conn_t *c) {
    if (!c) return;
    close(c->fd);
    free(c);
}

void rp_unlisten(rp_listener_t *l) {
    if (!l) return;
    close(l->fd);
    unlink(l->path);
    free(l);
}

/* ---- worker endpoint (client side) ---- */

int rp_connect(const char *endpoint, rp_conn_t **out) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_un a;
    memset(&a, 0, sizeof a);
    a.sun_family = AF_UNIX;
    if (strlen(endpoint) >= sizeof a.sun_path) { close(fd); return -1; }
    strcpy(a.sun_path, endpoint);
    if (connect(fd, (struct sockaddr *)&a, sizeof a) < 0) {
        close(fd);
        return -1;
    }
    rp_conn_t *c = calloc(1, sizeof *c);
    if (!c) { close(fd); return -1; }
    c->fd = fd;
    *out = c;
    return 0;
}

int rp_request(rp_conn_t *c, const rp_msg_deliver_t *req,
               rp_msg_reply_t *resp, int timeout_ms) {
    if (write_full(c->fd, req, sizeof *req) < 0)
        return RP_SEND_LOST_REPLY;
    long long deadline = rp_now_ms() + timeout_ms;
    int r;
    do {
        int remain = (int)(deadline - rp_now_ms());
        if (remain <= 0) return RP_SEND_TIMEDOUT;
        r = wait_io(c->fd, POLLIN, remain);
    } while (r == -1 && errno == EINTR);
    if (r == 0) return RP_SEND_TIMEDOUT;
    if (r < 0) return RP_SEND_LOST_REPLY;           /* POLLHUP/ERR */
    int rr = read_full(c->fd, resp, sizeof *resp);
    if (rr <= 0) return RP_SEND_LOST_REPLY;         /* EOF: worker died */
    return RP_SEND_OK;
}

void rp_disconnect(rp_conn_t *c) {
    if (!c) return;
    close(c->fd);
    free(c);
}

#endif /* !__QNXNTO__ */
