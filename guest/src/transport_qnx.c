/* QNX 8 native transport: registered channel names and synchronous
 * message passing (name_attach / name_open / MsgSend / MsgReceive /
 * MsgReply). Compiled only on __QNXNTO__ — this file is the measured
 * target path; a POSIX host build never satisfies the QNX gate.
 *
 * Semantics mirrored from the POSIX transport:
 *   - worker endpoint name is registered in the local namespace
 *     (e.g. "rp_spool_<run>"); a new generation re-attaches the name and
 *     becomes the fresh endpoint discovered by name_open.
 *   - a client blocked in MsgSend when the server dies observes
 *     ESRCH/ECONNRESET — recorded as LOST_REPLY, never inferred.
 */
#ifdef __QNXNTO__

#include "rp_transport.h"
#include "rp_common.h"

#include <sys/neutrino.h>
#include <sys/dispatch.h>
#include <sys/netmgr.h>
#include <sys/iofunc.h>
#include <errno.h>
#include <time.h>

struct rp_listener {
    name_attach_t *attach;
    int            chid;
    char           name[160];
};

struct rp_conn {
    int coid;   /* client: connection id */
    int rcvid;  /* server: receive id token for the pending reply */
};

int rp_listen(const char *endpoint, rp_listener_t **out) {
    rp_listener_t *l = calloc(1, sizeof *l);
    if (!l) return -1;
    l->attach = name_attach(NULL, endpoint, 0);
    if (!l->attach) { free(l); return -1; }
    l->chid = l->attach->chid;
    strncpy(l->name, endpoint, sizeof l->name - 1);
    *out = l;
    return 0;
}

int rp_listen_recv(rp_listener_t *l, rp_msg_deliver_t *out, rp_conn_t **cli,
                   int timeout_ms) {
    struct _msg_info info;
    if (timeout_ms >= 0) {
        struct timespec to = { timeout_ms / 1000,
                               (long)(timeout_ms % 1000) * 1000000L };
        TimerTimeout(CLOCK_MONOTONIC, _NTO_TIMEOUT_RECEIVE, NULL, &to, NULL);
    }
    int rcvid = MsgReceive(l->chid, out, sizeof *out, &info);
    if (rcvid < 0) {
        if (errno == ETIMEDOUT) return 0;
        return -1;
    }
    if (rcvid == 0) return -2;           /* pulse — not a request */
    rp_conn_t *c = calloc(1, sizeof *c);
    if (!c) return -1;
    c->rcvid = rcvid;
    *cli = c;
    return 0;
}

int rp_conn_reply(rp_conn_t *c, const rp_msg_reply_t *msg) {
    return MsgReply(c->rcvid, EOK, msg, sizeof *msg);
}

void rp_conn_close(rp_conn_t *c) {
    free(c);    /* nothing else to release; rcvid is consumed by reply */
}

void rp_unlisten(rp_listener_t *l) {
    if (!l) return;
    name_detach(l->attach, l->chid);
    free(l);
}

int rp_connect(const char *endpoint, rp_conn_t **out) {
    int coid = name_open(endpoint, 0);
    if (coid < 0) return -1;
    rp_conn_t *c = calloc(1, sizeof *c);
    if (!c) { name_close(coid); return -1; }
    c->coid = coid;
    *out = c;
    return 0;
}

int rp_request(rp_conn_t *c, const rp_msg_deliver_t *req,
               rp_msg_reply_t *resp, int timeout_ms) {
    if (timeout_ms >= 0) {
        struct timespec to = { timeout_ms / 1000,
                               (long)(timeout_ms % 1000) * 1000000L };
        TimerTimeout(CLOCK_MONOTONIC,
                     _NTO_TIMEOUT_SEND | _NTO_TIMEOUT_REPLY,
                     NULL, &to, NULL);
    }
    int rc = MsgSend(c->coid, req, sizeof *req, resp, sizeof *resp);
    if (rc < 0) {
        if (errno == ETIMEDOUT) return RP_SEND_TIMEDOUT;
        if (errno == ESRCH)     return RP_SEND_LOST_REPLY;
        return RP_SEND_LOST_REPLY;
    }
    return RP_SEND_OK;
}

void rp_disconnect(rp_conn_t *c) {
    if (!c) return;
    name_close(c->coid);
    free(c);
}

#endif /* __QNXNTO__ */
