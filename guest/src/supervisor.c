/* RestartProof supervisor — owns exactly one disposable worker image.
 *
 * Spawns the worker generation sequence, records generation changes,
 * holds worker barrier connections, and only ever signals its own
 * registered child processes. Control requests arrive on a TCP channel
 * (reachable from the host runner) carrying the run nonce and a
 * monotonic control generation; stale or foreign requests are rejected
 * and journaled.
 */
#include "rp_protocol.h"
#include "rp_transport.h"
#include "rp_common.h"

#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <sys/utsname.h>

extern char **environ;

#define MAX_GEN 8

typedef struct {
    const char *rundir, *worker_image, *run, *nonce, *sink, *wtok;
    const char *endpoint_base;   /* path or name prefix for worker eps */
    int ctl_port;
    pid_t worker_pid[MAX_GEN + 1];
    int held_fd[MAX_GEN + 1];    /* ctl conn of worker held at barrier */
    int generation;
    uint32_t last_control_gen;
    int listen_fd;
} sup_t;

static void usage(void) {
    fprintf(stderr,
        "rp_supervisor --rundir DIR --worker-image PATH --run ID\n"
        "    --nonce N --sink URL --writer-token T --ctl-port P\n"
        "    --endpoint-base BASE\n");
}

static void write_registry(sup_t *s) {
    char ep[600], buf[1600], path[1100];
#ifdef __QNXNTO__
    snprintf(ep, sizeof ep, "%s", s->endpoint_base);
#else
    snprintf(ep, sizeof ep, "%s/worker_g%d.end", s->endpoint_base,
             s->generation);
#endif
    snprintf(buf, sizeof buf,
        "{\"generation\":%d,\"endpoint\":\"%s\",\"workerPid\":%d,"
        "\"runNonce\":\"%s\"}\n",
        s->generation, ep, (int)s->worker_pid[s->generation], s->nonce);
    snprintf(path, sizeof path, "%s/registry.json", s->rundir);
    rp_write_file_atomic(path, buf);
}

static int spawn_worker(sup_t *s) {
    char gen_s[8], port_s[8], ep[600];
    snprintf(gen_s, sizeof gen_s, "%d", s->generation);
    snprintf(port_s, sizeof port_s, "%d", s->ctl_port);
#ifdef __QNXNTO__
    snprintf(ep, sizeof ep, "%s", s->endpoint_base);
#else
    snprintf(ep, sizeof ep, "%s/worker_g%d.end", s->endpoint_base,
             s->generation);
#endif
    char *argv[] = {
        (char *)s->worker_image,
        "--endpoint", ep,
        "--ctl-host", "127.0.0.1",
        "--ctl-port", port_s,
        "--gen", gen_s,
        "--run", (char *)s->run,
        "--nonce", (char *)s->nonce,
        "--sink", (char *)s->sink,
        "--writer-token", (char *)s->wtok,
        "--rundir", (char *)s->rundir,
        NULL
    };
    pid_t pid;
    int rc = posix_spawn(&pid, s->worker_image, NULL, NULL, argv, environ);
    if (rc != 0) return -1;
    s->worker_pid[s->generation] = pid;
    rp_event(s->rundir,
        "{\"kind\":\"worker_spawn\",\"gen\":%d,\"pid\":%d}",
        s->generation, (int)pid);
    write_registry(s);
    return 0;
}

static void reply(int fd, int status, uint32_t gen, const char *detail) {
    rp_msg_ctl_reply_t r;
    memset(&r, 0, sizeof r);
    r.protocol_version = RP_PROTOCOL_VERSION;
    r.status = status;
    r.generation = gen;
    strncpy(r.detail, detail, sizeof r.detail - 1);
    rp_ctl_reply(fd, &r);
}

static int stop_worker(sup_t *s, int gen) {
    pid_t pid = s->worker_pid[gen];
    if (pid <= 0) return -1;
    kill(pid, SIGKILL);              /* only our own registered child */
    int st;
    while (waitpid(pid, &st, 0) < 0 && errno == EINTR) {}
    s->worker_pid[gen] = 0;
    rp_event(s->rundir,
        "{\"kind\":\"worker_stopped\",\"gen\":%d,\"pid\":%d}", gen,
        (int)pid);
    return 0;
}

/* read a whole file (bounded); NULL if absent */
static char *read_whole(const char *path, size_t cap) {
    FILE *f = fopen(path, "r");
    if (!f) return NULL;
    char *b = malloc(cap + 1);
    if (!b) { fclose(f); return NULL; }
    size_t n = fread(b, 1, cap, f);
    fclose(f);
    b[n] = 0;
    return b;
}

#define RP_EV_CAP (192 * 1024)

/* minimal JSON string escaping (quotes + backslashes + controls) */
static void json_escape(const char *in, char *out, size_t cap) {
    size_t o = 0;
    for (const char *p = in; *p && o + 2 < cap; p++) {
        unsigned char c = (unsigned char)*p;
        if (c == '"' || c == '\\') {
            out[o++] = '\\'; out[o++] = (char)c;
        } else if (c < 0x20) {
            out[o++] = ' ';
        } else {
            out[o++] = (char)c;
        }
    }
    out[o] = 0;
}

/* build {"uname":"…","events":[…],"clientReport":<obj|null>} for the
 * operator bridge (RP_CTL_EVENTS). events.jsonl lines are already JSON
 * objects; the client report embeds verbatim when present. */
static char *gather_evidence(sup_t *s) {
    char path[1100];
    snprintf(path, sizeof path, "%s/events.jsonl", s->rundir);
    char *raw = read_whole(path, RP_EV_CAP);
    size_t ev_len = raw ? strlen(raw) : 0;
    char *events = malloc(ev_len + 4);
    if (events) {
        char *o = events;
        *o++ = '[';
        int first = 1;
        if (raw) {
            char *line = raw, *end;
            while (*line) {
                end = strchr(line, '\n');
                size_t n = end ? (size_t)(end - line) : strlen(line);
                while (n && (line[n-1] == '\r' || line[n-1] == ' ')) n--;
                if (n && line[0] == '{') {
                    if (!first) *o++ = ',';
                    memcpy(o, line, n); o += n;
                    first = 0;
                }
                line = end ? end + 1 : line + n;
            }
        }
        *o++ = ']'; *o = 0;
    }

    snprintf(path, sizeof path, "%s/client_report.json", s->rundir);
    char *rep = read_whole(path, 96 * 1024);
    if (rep) {
        size_t n = strlen(rep);
        while (n && (rep[n-1] == '\n' || rep[n-1] == ' '
                     || rep[n-1] == '\r')) rep[--n] = 0;
        if (rep[0] != '{') { free(rep); rep = NULL; }
    }

    char uname_esc[640] = "unknown";
    struct utsname u;
    if (uname(&u) == 0) {
        char ubuf[512];
        snprintf(ubuf, sizeof ubuf, "%s %s %s %s",
                 u.sysname, u.release, u.version, u.machine);
        json_escape(ubuf, uname_esc, sizeof uname_esc);
    }

    size_t total = (events ? strlen(events) : 2)
                 + (rep ? strlen(rep) : 4) + strlen(uname_esc) + 96;
    char *json = malloc(total);
    if (json) {
        snprintf(json, total,
            "{\"uname\":\"%s\",\"events\":%s,\"clientReport\":%s}",
            uname_esc, events ? events : "[]", rep ? rep : "null");
    }
    free(raw); free(events); free(rep);
    return json;
}

static void handle_ctl(sup_t *s, int fd, const rp_msg_ctl_t *m) {
    if (m->protocol_version != RP_PROTOCOL_VERSION) {
        reply(fd, RP_CTL_ERR_BAD_MSG, s->generation, "bad version");
        rp_event(s->rundir,
            "{\"kind\":\"ctl_reject\",\"reason\":\"bad_version\"}");
        return;
    }
    if (m->kind == RP_CTL_BARRIER) {
        /* worker announcing its post-commit hold; nonce-checked */
        if (!rp_streq(m->run_nonce, s->nonce)) {
            reply(fd, RP_CTL_ERR_NONCE, s->generation, "nonce mismatch");
            rp_event(s->rundir,
                "{\"kind\":\"ctl_reject\",\"reason\":\"nonce\"}");
            return;
        }
        int g = (int)m->generation;
        if (g < 1 || g > MAX_GEN || g != s->generation) {
            reply(fd, RP_CTL_ERR_STALE, s->generation,
                  "barrier for non-current generation");
            rp_event(s->rundir,
                "{\"kind\":\"ctl_reject\",\"reason\":\"stale_barrier\","
                "\"gen\":%d}", g);
            return;
        }
        if (s->held_fd[g] >= 0) close(s->held_fd[g]);
        s->held_fd[g] = fd;      /* keep open; worker is holding on it */
        rp_event(s->rundir,
            "{\"kind\":\"barrier\",\"gen\":%d,\"opId\":\"%s\","
            "\"attemptId\":\"%s\",\"seq\":%lld}",
            g, m->op_id, m->attempt_id, (long long)m->receipt_seq);
        return;                  /* no reply: the hold stays held */
    }

    /* runner commands require the run nonce and fresh control gen */
    if (!rp_streq(m->run_nonce, s->nonce)) {
        reply(fd, RP_CTL_ERR_NONCE, s->generation, "nonce mismatch");
        rp_event(s->rundir, "{\"kind\":\"ctl_reject\",\"reason\":\"nonce\"}");
        return;
    }
    if (m->kind == RP_CTL_STATUS) {
        char d[64];
        snprintf(d, sizeof d, "generation %d", s->generation);
        reply(fd, RP_CTL_OK, s->generation, d);
        return;
    }
    if (m->kind == RP_CTL_EVENTS) {
        /* read-only gather for the operator bridge; nonce already
         * verified, no control-generation draw (like STATUS) */
        char *json = gather_evidence(s);
        reply(fd, json ? RP_CTL_OK : RP_CTL_ERR_INTERNAL,
              s->generation, "evidence");
        rp_ctl_send_blob(fd, json ? json : "{}",
                         (uint32_t)(json ? strlen(json) : 2));
        free(json);
        return;
    }
    if (m->control_generation <= s->last_control_gen) {
        reply(fd, RP_CTL_ERR_STALE, s->generation,
              "stale control generation");
        rp_event(s->rundir,
            "{\"kind\":\"ctl_reject\",\"reason\":\"stale_control\","
            "\"controlGeneration\":%u}", m->control_generation);
        return;
    }
    s->last_control_gen = m->control_generation;

    if (m->kind == RP_CTL_RESTART) {
        int g = s->generation;
        if (s->held_fd[g] < 0) {
            reply(fd, RP_CTL_ERR_NO_BARRIER, g,
                  "worker is not held at the barrier");
            rp_event(s->rundir,
                "{\"kind\":\"ctl_reject\",\"reason\":\"no_barrier\"}");
            return;
        }
        close(s->held_fd[g]);
        s->held_fd[g] = -1;
        stop_worker(s, g);
        if (s->generation >= MAX_GEN) {
            reply(fd, RP_CTL_ERR_INTERNAL, g, "generation limit");
            return;
        }
        s->generation++;
        if (spawn_worker(s) < 0) {
            reply(fd, RP_CTL_ERR_INTERNAL, s->generation, "spawn failed");
            return;
        }
        reply(fd, RP_CTL_OK, s->generation, "restarted");
        rp_event(s->rundir,
            "{\"kind\":\"restart\",\"fromGen\":%d,\"toGen\":%d}",
            g, s->generation);
        return;
    }
    if (m->kind == RP_CTL_CONTINUE) {
        int g = s->generation;
        if (s->held_fd[g] >= 0) {
            reply(s->held_fd[g], RP_CTL_OK, g, "continue");
            close(s->held_fd[g]);
            s->held_fd[g] = -1;
        }
        reply(fd, RP_CTL_OK, s->generation, "released");
        return;
    }
    if (m->kind == RP_CTL_STOP) {
        reply(fd, RP_CTL_OK, s->generation, "stopping");
        int g = s->generation;
        if (s->held_fd[g] >= 0) close(s->held_fd[g]);
        stop_worker(s, g);
        rp_event(s->rundir, "{\"kind\":\"supervisor_stop\"}");
        close(s->listen_fd);
        exit(0);
    }
    reply(fd, RP_CTL_ERR_BAD_MSG, s->generation, "unknown kind");
}

int main(int argc, char **argv) {
    sup_t s;
    memset(&s, 0, sizeof s);
    s.ctl_port = 0;
    s.generation = 1;
    for (int i = 0; i <= MAX_GEN; i++) s.held_fd[i] = -1;
    for (int i = 1; i + 1 < argc; i += 2) {
        if (rp_streq(argv[i], "--rundir"))          s.rundir = argv[i+1];
        else if (rp_streq(argv[i], "--worker-image")) s.worker_image = argv[i+1];
        else if (rp_streq(argv[i], "--run"))        s.run = argv[i+1];
        else if (rp_streq(argv[i], "--nonce"))      s.nonce = argv[i+1];
        else if (rp_streq(argv[i], "--sink"))       s.sink = argv[i+1];
        else if (rp_streq(argv[i], "--writer-token")) s.wtok = argv[i+1];
        else if (rp_streq(argv[i], "--ctl-port"))   s.ctl_port = atoi(argv[i+1]);
        else if (rp_streq(argv[i], "--endpoint-base")) s.endpoint_base = argv[i+1];
    }
    if (!s.rundir || !s.worker_image || !s.run || !s.nonce || !s.sink
            || !s.wtok || !s.endpoint_base) {
        usage();
        return 2;
    }
    /* the supervisor only ever drives this one allowlisted image */
    if (access(s.worker_image, X_OK) != 0) {
        fprintf(stderr, "worker image not executable: %s\n",
                s.worker_image);
        return 3;
    }

    s.listen_fd = -1;
    int port = rp_ctl_listen(s.ctl_port, &s.listen_fd);
    if (port < 0) {
        fprintf(stderr, "ctl listen failed\n");
        return 3;
    }
    s.ctl_port = port;
    char path[1100], info[512];
    snprintf(path, sizeof path, "%s/ctl.json", s.rundir);
    snprintf(info, sizeof info,
             "{\"ctlPort\":%d,\"pid\":%d,\"runNonce\":\"%s\"}\n",
             port, (int)getpid(), s.nonce);
    rp_write_file_atomic(path, info);

    if (spawn_worker(&s) < 0) {
        fprintf(stderr, "worker spawn failed\n");
        return 3;
    }

    for (;;) {
        int fd = -1;
        int r = rp_ctl_accept(s.listen_fd, &fd, -1);
        if (r != 0) continue;
        rp_msg_ctl_t m;
        int rr = rp_ctl_recv(fd, &m, 15000);
        if (rr == 0) {
            handle_ctl(&s, fd, &m);
            /* barrier conns are kept; all others close after handling */
            int kept = 0;
            for (int g = 0; g <= MAX_GEN; g++)
                if (s.held_fd[g] == fd) kept = 1;
            if (!kept) close(fd);
        } else {
            close(fd);
        }
    }
}
