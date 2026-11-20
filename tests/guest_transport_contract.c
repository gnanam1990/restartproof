/* Host contract test for the portable transport conventions.
 *
 * Compiled and run by tests/test_m03_harness.py (cc on PATH). Verifies
 * the same return-code contract the QNX transport implements:
 *   rp_listen_recv: 0 success (cli set), 1 timeout, <0 error.
 *   ctl_frame_read path: 0 frame, 1 timeout, <0 error.
 * Exits nonzero with a message on any violation.
 */
#include "rp_transport.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static int fails = 0;
#define CHECK(cond, msg) do { \
    if (!(cond)) { fprintf(stderr, "FAIL: %s\n", msg); fails++; } \
} while (0)

int main(int argc, char **argv) {
    const char *dir = argc > 1 ? argv[1] : "/tmp";
    char ep[256];
    snprintf(ep, sizeof ep, "%s/rp_contract.end", dir);

    /* 1. listen timeout: must return 1 and leave cli NULL */
    rp_listener_t *l = NULL;
    CHECK(rp_listen(ep, &l) == 0, "rp_listen failed");
    rp_msg_deliver_t req;
    rp_conn_t *cli = NULL;
    int r = rp_listen_recv(l, &req, &cli, 150);
    CHECK(r == 1, "listen timeout must return 1");
    CHECK(cli == NULL, "timeout must not set cli");

    /* 2. ctl timeout: 1, not success and not error */
    int cfd;
    int port = rp_ctl_listen(0, &cfd);
    CHECK(port > 0, "rp_ctl_listen failed");
    int afd;
    CHECK(rp_ctl_accept(cfd, &afd, 150) == 1, "accept timeout != 1");
    int pfd;
    CHECK(rp_ctl_connect("127.0.0.1", port, &pfd) == 0,
          "rp_ctl_connect failed");
    CHECK(rp_ctl_accept(cfd, &afd, 3000) == 0, "accept failed");
    rp_msg_ctl_t m;
    CHECK(rp_ctl_recv(afd, &m, 150) == 1, "ctl recv timeout != 1");

    /* 3. ctl roundtrip: framed request + reply-sized response */
    rp_msg_ctl_t req_msg;
    memset(&req_msg, 0, sizeof req_msg);
    req_msg.protocol_version = RP_PROTOCOL_VERSION;
    req_msg.kind = RP_CTL_STATUS;
    strncpy(req_msg.run_nonce, "nonce0123456789ab",
            sizeof req_msg.run_nonce - 1);
    CHECK(rp_ctl_send(pfd, &req_msg) == 0, "ctl_send failed");
    rp_msg_ctl_t got;
    CHECK(rp_ctl_recv(afd, &got, 3000) == 0, "ctl_recv failed");
    CHECK(got.kind == RP_CTL_STATUS, "ctl kind mismatch");

    rp_msg_ctl_reply_t rep;
    memset(&rep, 0, sizeof rep);
    rep.protocol_version = RP_PROTOCOL_VERSION;
    rep.status = RP_CTL_OK;
    rep.generation = 7;
    CHECK(rp_ctl_reply(afd, &rep) == 0, "ctl_reply failed");
    rp_msg_ctl_reply_t got_r;
    CHECK(rp_ctl_recv_reply(pfd, &got_r, 3000) == 0,
          "ctl_recv_reply failed");
    CHECK(got_r.generation == 7 && got_r.status == RP_CTL_OK,
          "ctl reply fields mismatch");

    close(pfd); close(afd); close(cfd);
    rp_unlisten(l);

    if (fails) { fprintf(stderr, "%d contract checks failed\n", fails);
                 return 1; }
    puts("transport contract: OK");
    return 0;
}
