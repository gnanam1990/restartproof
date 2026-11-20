/* RestartProof test worker — the disposable label-spool service.
 *
 * Accepts one delivery at a time on its registered endpoint, commits it
 * to the independent receipt sink over HTTP, then — when the request
 * asks — signals the post-commit/pre-reply barrier to its supervisor and
 * holds without replying. It never classifies anything; it only reports
 * what the sink durably committed.
 */
#include "rp_protocol.h"
#include "rp_transport.h"
#include "rp_http.h"
#include "rp_common.h"

static void usage(void) {
    fprintf(stderr,
        "rp_worker --endpoint EP --ctl-host H --ctl-port P --gen N\n"
        "          --run ID --nonce N --sink URL --writer-token T\n"
        "          --rundir DIR [--barrier-timeout-ms N]\n");
}

int main(int argc, char **argv) {
    const char *endpoint = NULL, *ctl_host = "127.0.0.1";
    const char *run = NULL, *nonce = NULL, *sink = NULL;
    const char *wtok = NULL, *rundir = ".";
    int ctl_port = 0, gen = 0, barrier_ms = 30000;
    for (int i = 1; i + 1 < argc; i += 2) {
        if (rp_streq(argv[i], "--endpoint"))           endpoint = argv[i+1];
        else if (rp_streq(argv[i], "--ctl-host"))      ctl_host = argv[i+1];
        else if (rp_streq(argv[i], "--ctl-port"))      ctl_port = atoi(argv[i+1]);
        else if (rp_streq(argv[i], "--gen"))           gen = atoi(argv[i+1]);
        else if (rp_streq(argv[i], "--run"))           run = argv[i+1];
        else if (rp_streq(argv[i], "--nonce"))         nonce = argv[i+1];
        else if (rp_streq(argv[i], "--sink"))          sink = argv[i+1];
        else if (rp_streq(argv[i], "--writer-token"))  wtok = argv[i+1];
        else if (rp_streq(argv[i], "--rundir"))        rundir = argv[i+1];
        else if (rp_streq(argv[i], "--barrier-timeout-ms"))
            barrier_ms = atoi(argv[i+1]);
    }
    if (!endpoint || !ctl_port || !run || !nonce || !sink || !wtok) {
        usage();
        return 2;
    }

    rp_listener_t *l = NULL;
    if (rp_listen(endpoint, &l) < 0) {
        rp_event(rundir,
                 "{\"kind\":\"worker_error\",\"gen\":%d,\"reason\":"
                 "\"listen_failed\"}", gen);
        return 3;
    }
    rp_event(rundir,
             "{\"kind\":\"worker_ready\",\"gen\":%d,\"pid\":%d}", gen,
             (int)getpid());

    for (;;) {
        rp_msg_deliver_t req;
        rp_conn_t *cli = NULL;
        int r = rp_listen_recv(l, &req, &cli, -1);
        if (r != 0) continue;

        rp_msg_reply_t resp;
        memset(&resp, 0, sizeof resp);
        resp.protocol_version = RP_PROTOCOL_VERSION;
        resp.status = -1;
        resp.receipt_seq = -1;

        if (req.protocol_version != RP_PROTOCOL_VERSION ||
            !rp_streq(req.run_nonce, nonce) || !rp_streq(req.run_id, run)) {
            snprintf(resp.detail, sizeof resp.detail,
                     "rejected: protocol/nonce/run mismatch");
            rp_conn_reply(cli, &resp);
            rp_conn_close(cli);
            continue;
        }

        /* commit to the independent sink FIRST */
        char url[512], body[1600], http_resp[4096];
        snprintf(url, sizeof url, "%s/v1/runs/%s/deliver", sink, run);
        snprintf(body, sizeof body,
            "{\"runNonce\":\"%s\",\"opId\":\"%s\",\"attemptId\":\"%s\","
            "\"payloadDigest\":\"%s\",\"payload\":%s}",
            nonce, req.op_id, req.attempt_id, req.payload_digest,
            req.payload_json[0] ? req.payload_json : "null");
        int http_status = 0;
        int hr = rp_http_post(url, wtok, body, 15000, &http_status,
                              http_resp, sizeof http_resp);
        if (hr != 0 || http_status != 200) {
            resp.status = -2;
            snprintf(resp.detail, sizeof resp.detail,
                     "sink delivery failed (http %d rc %d)",
                     http_status, hr);
            rp_conn_reply(cli, &resp);
            rp_conn_close(cli);
            rp_event(rundir,
                     "{\"kind\":\"worker_sink_error\",\"gen\":%d,"
                     "\"http\":%d}", gen, http_status);
            continue;
        }
        long long seq = -1;
        rp_json_field_ll(http_resp, "seq", &seq);
        rp_json_field_str(http_resp, "sinkId", resp.sink_id,
                          sizeof resp.sink_id);
        resp.status = 0;
        resp.receipt_seq = seq;
        snprintf(resp.detail, sizeof resp.detail, "delivered");
        rp_event(rundir,
            "{\"kind\":\"worker_committed\",\"gen\":%d,"
            "\"attemptId\":\"%s\",\"seq\":%lld}", gen, req.attempt_id, seq);

        if (!req.hold_at_barrier) {
            rp_conn_reply(cli, &resp);
            rp_conn_close(cli);
            continue;
        }

        /* post-commit / pre-reply barrier: tell the supervisor, then
         * hold WITHOUT replying to the client (bounded). */
        int ctl_fd = -1;
        if (rp_ctl_connect(ctl_host, ctl_port, &ctl_fd) == 0) {
            rp_msg_ctl_t b;
            memset(&b, 0, sizeof b);
            b.protocol_version = RP_PROTOCOL_VERSION;
            b.kind = RP_CTL_BARRIER;
            b.generation = (uint32_t)gen;
            b.receipt_seq = seq;
            strncpy(b.run_nonce, nonce, sizeof b.run_nonce - 1);
            strncpy(b.op_id, req.op_id, sizeof b.op_id - 1);
            strncpy(b.attempt_id, req.attempt_id,
                    sizeof b.attempt_id - 1);
            rp_ctl_send(ctl_fd, &b);
            /* hold: wait for a ctl reply (CONTINUE ack) or the conn to
             * drop; the supervisor may also just stop us — either way
             * the hold is bounded */
            rp_msg_ctl_reply_t ack;
            int br = rp_ctl_recv_reply(ctl_fd, &ack, barrier_ms);
            close(ctl_fd);
            if (br == 0)
                rp_event(rundir,
                    "{\"kind\":\"worker_released\",\"gen\":%d}", gen);
            else if (br == 1)
                rp_event(rundir,
                    "{\"kind\":\"worker_barrier_timeout\",\"gen\":%d}",
                    gen);
            else
                rp_event(rundir,
                    "{\"kind\":\"worker_barrier_lost\",\"gen\":%d}", gen);
        } else {
            rp_event(rundir,
                "{\"kind\":\"worker_barrier_noctl\",\"gen\":%d}", gen);
            rp_msleep(barrier_ms < 5000 ? barrier_ms : 5000);
        }
        /* if we are still alive past the barrier, answer the client */
        rp_conn_reply(cli, &resp);
        rp_conn_close(cli);
    }
}
