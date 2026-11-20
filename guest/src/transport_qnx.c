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
    /* flags 0: register in the LOCAL namespace — the endpoint must not
     * leak into a global Qnet namespace where foreign senders could
     * reach the measured boundary */
    l->attach = name_attach(NULL, endpoint, 0);
    if (!l->attach) { free(l); return -1; }
    l->chid = l->attach->chid;
    strncpy(l->name, endpoint, sizeof l->name - 1);
    *out = l;
    return 0;
}

int rp_listen_recv(rp_listener_t *l, rp_msg_deliver_t *out, rp_conn_t **cli,
                   int timeout_ms) {
    /* returns 0 with *cli set, 1 on timeout, <0 on error */
    struct _msg_info info;
    if (timeout_ms >= 0) {
        /* TimerTimeout ntime is a uint64_t of RELATIVE NANOSECONDS */
        uint64_t ns = (uint64_t)timeout_ms * 1000000ULL;
        if (TimerTimeout(CLOCK_MONOTONIC, _NTO_TIMEOUT_RECEIVE, NULL,
                         &ns, NULL) == -1)
            return -5;                   /* could not arm the bound */
    }
    int rcvid = MsgReceive(l->chid, out, sizeof *out, &info);
    if (rcvid < 0) {
        if (errno == ETIMEDOUT) return 1;
        return -1;
    }
    if (rcvid == 0) return -2;           /* pulse — not a request */
    if (info.msglen != sizeof *out) {
        /* short/oversized payload: unblock the sender with an error so
         * it records LOST_REPLY; never let a partial struct reach the
         * measured boundary */
        MsgError(rcvid, EINVAL);
        return -3;
    }
    rp_conn_t *c = calloc(1, sizeof *c);
    if (!c) { MsgError(rcvid, ENOMEM); return -1; }
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
    /* name_detach(attach, flags): flags 0 removes the name AND destroys
     * the channel + dispatch handle (default semantics — we did not
     * create the dpp ourselves, so SAVEDPP does not apply); blocked
     * senders unblock with an error */
    if (name_detach(l->attach, 0) == -1) {
        /* process teardown cleans up regardless; nothing to report to */
    }
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
        /* uint64_t relative nanoseconds — covers SEND and REPLY block */
        uint64_t ns = (uint64_t)timeout_ms * 1000000ULL;
        if (TimerTimeout(CLOCK_MONOTONIC,
                         _NTO_TIMEOUT_SEND | _NTO_TIMEOUT_REPLY,
                         NULL, &ns, NULL) == -1)
            return RP_SEND_INTERNAL;     /* bound could not be armed */
    }
    int rc = MsgSend(c->coid, req, sizeof *req, resp, sizeof *resp);
    if (rc < 0) {
        if (errno == ETIMEDOUT) return RP_SEND_TIMEDOUT;
        return RP_SEND_LOST_REPLY;   /* ESRCH/ECONNRESET: peer gone */
    }
    return RP_SEND_OK;
}

void rp_disconnect(rp_conn_t *c) {
    if (!c) return;
    name_close(c->coid);
    free(c);
}

#endif /* __QNXNTO__ */
