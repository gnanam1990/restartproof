/* RestartProof guest client — the synchronous sender whose view of the
 * world is exactly what a MsgSend caller sees.
 *
 * Sends the operation once per attempt with a bounded wait. On a lost
 * reply it does NOT classify anything; it rediscovers the (fresh)
 * endpoint from the supervisor registry and retries the same operation
 * with a new attempt id, recording every raw send result.
 */
#include "rp_protocol.h"
#include "rp_transport.h"
#include "rp_common.h"

static void usage(void) {
    fprintf(stderr,
        "rp_client --rundir DIR --run ID --nonce N --op OPID\n"
        "    --payload-json JSON --payload-digest D\n"
        "    --attempt-prefix P [--max-attempts N] [--timeout-ms N]\n"
        "    [--discover-timeout-ms N] [--hold-first 0|1] --report PATH\n");
}

static const char *send_result_name(int r) {
    switch (r) {
        case RP_SEND_OK:             return "OK";
        case RP_SEND_LOST_REPLY:     return "LOST_REPLY";
        case RP_SEND_TIMEDOUT:       return "ETIMEDOUT";
        case RP_SEND_CONNECT_FAILED: return "CONNECT_FAILED";
        case RP_SEND_NO_ENDPOINT:    return "NO_ENDPOINT";
        case RP_SEND_INTERNAL:       return "TRANSPORT_ERR";
        default:                     return "UNKNOWN";
    }
}

static int read_registry(const char *rundir, char *ep, int epsz,
                         int *gen) {
    char path[1100], buf[2048], epbuf[512];
    long long g;
    snprintf(path, sizeof path, "%s/registry.json", rundir);
    if (rp_read_file(path, buf, sizeof buf) <= 0) return -1;
    if (rp_json_field_str(buf, "endpoint", epbuf, sizeof epbuf) < 0)
        return -1;
    if (rp_json_field_ll(buf, "generation", &g) < 0) return -1;
    strncpy(ep, epbuf, epsz - 1);
    ep[epsz - 1] = 0;
    *gen = (int)g;
    return 0;
}

int main(int argc, char **argv) {
    const char *rundir = NULL, *run = NULL, *nonce = NULL, *op = NULL;
    const char *payload = NULL, *pdigest = NULL, *prefix = "att";
    const char *report = NULL;
    int max_attempts = 2, timeout_ms = 8000, discover_ms = 15000;
    int hold_first = 1;
    for (int i = 1; i + 1 < argc; i += 2) {
        if (rp_streq(argv[i], "--rundir"))          rundir = argv[i+1];
        else if (rp_streq(argv[i], "--run"))       run = argv[i+1];
        else if (rp_streq(argv[i], "--nonce"))     nonce = argv[i+1];
        else if (rp_streq(argv[i], "--op"))        op = argv[i+1];
        else if (rp_streq(argv[i], "--payload-json")) payload = argv[i+1];
        else if (rp_streq(argv[i], "--payload-digest")) pdigest = argv[i+1];
        else if (rp_streq(argv[i], "--attempt-prefix")) prefix = argv[i+1];
        else if (rp_streq(argv[i], "--max-attempts")) max_attempts = atoi(argv[i+1]);
        else if (rp_streq(argv[i], "--timeout-ms")) timeout_ms = atoi(argv[i+1]);
        else if (rp_streq(argv[i], "--discover-timeout-ms")) discover_ms = atoi(argv[i+1]);
        else if (rp_streq(argv[i], "--hold-first")) hold_first = atoi(argv[i+1]);
        else if (rp_streq(argv[i], "--report"))    report = argv[i+1];
    }
    if (!rundir || !run || !nonce || !op || !payload || !pdigest
            || !report) {
        usage();
        return 2;
    }

    char attempts_json[4096];
    size_t used = 0;
    used += snprintf(attempts_json + used,
                     sizeof attempts_json - used, "[");
    int completed = 0, last_gen = -1;

    for (int a = 0; a < max_attempts; a++) {
        char ep[512] = {0}, attempt[80];
        int gen = -1;
        snprintf(attempt, sizeof attempt, "%s-%d", prefix, a + 1);

        /* discover the current endpoint (fresh after a restart) */
        long long deadline = rp_now_ms() + discover_ms;
        int found = -1;
        while (rp_now_ms() < deadline) {
            found = read_registry(rundir, ep, sizeof ep, &gen);
            if (found == 0 && gen != last_gen) break;
            if (found == 0 && a == 0) break;   /* first attempt: any gen */
            rp_msleep(50);
        }
        if (found != 0 || (a > 0 && gen == last_gen)) {
            /* no usable endpoint, or only the previous (dying)
             * generation's socket — never send to a stale worker */
            used += snprintf(attempts_json + used,
                sizeof attempts_json - used,
                "%s{\"attemptId\":\"%s\",\"generation\":-1,"
                "\"sendResult\":\"NO_ENDPOINT\"}", a ? "," : "", attempt);
            continue;
        }
        last_gen = gen;

        rp_msg_deliver_t req;
        memset(&req, 0, sizeof req);
        req.protocol_version = RP_PROTOCOL_VERSION;
        req.hold_at_barrier = (a == 0 && hold_first) ? 1 : 0;
        strncpy(req.run_id, run, sizeof req.run_id - 1);
        strncpy(req.run_nonce, nonce, sizeof req.run_nonce - 1);
        strncpy(req.op_id, op, sizeof req.op_id - 1);
        strncpy(req.attempt_id, attempt, sizeof req.attempt_id - 1);
        strncpy(req.payload_digest, pdigest,
                sizeof req.payload_digest - 1);
        strncpy(req.payload_json, payload, sizeof req.payload_json - 1);

        /* the registry can name a generation whose worker has not bound
         * its listener yet (spawn vs. listen race) — retry the connect,
         * bounded, before declaring CONNECT_FAILED */
        rp_conn_t *c = NULL;
        int sr;
        rp_msg_reply_t resp;
        memset(&resp, 0, sizeof resp);
        resp.receipt_seq = -1;
        long long cdeadline = rp_now_ms() + 3000;
        if (cdeadline > deadline) cdeadline = deadline;
        while (rp_connect(ep, &c) != 0) {
            if (rp_now_ms() >= cdeadline) break;
            rp_msleep(50);
        }
        if (c == NULL) {
            sr = RP_SEND_CONNECT_FAILED;
        } else {
            sr = rp_request(c, &req, &resp, timeout_ms);
            rp_disconnect(c);
        }
        used += snprintf(attempts_json + used,
            sizeof attempts_json - used,
            "%s{\"attemptId\":\"%s\",\"generation\":%d,"
            "\"sendResult\":\"%s\",\"receiptSeq\":%lld,"
            "\"replyStatus\":%d}",
            a ? "," : "", attempt, gen, send_result_name(sr),
            (long long)resp.receipt_seq, (int)resp.status);
        rp_event(rundir,
            "{\"kind\":\"attempt\",\"attemptId\":\"%s\",\"gen\":%d,"
            "\"sendResult\":\"%s\",\"receiptSeq\":%lld}",
            attempt, gen, send_result_name(sr),
            (long long)resp.receipt_seq);
        if (sr == RP_SEND_OK && resp.status == 0) {
            completed = 1;
            break;
        }
    }
    used += snprintf(attempts_json + used,
                     sizeof attempts_json - used, "]");

    char out[6000];
    snprintf(out, sizeof out,
        "{\"runId\":\"%s\",\"opId\":\"%s\",\"completed\":%s,"
        "\"attempts\":%s}\n",
        run, op, completed ? "true" : "false", attempts_json);
    if (rp_write_file_atomic(report, out) < 0) {
        fprintf(stderr, "cannot write report %s\n", report);
        return 3;
    }
    return completed ? 0 : 1;
}
