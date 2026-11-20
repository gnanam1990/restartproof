/* Small shared helpers for the guest harness (header-only). */
#ifndef RP_COMMON_H
#define RP_COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#if defined(__GNUC__) || defined(__clang__)
#  define RP_UNUSED_FN static __attribute__((unused))
#else
#  define RP_UNUSED_FN static
#endif

RP_UNUSED_FN int rp_streq(const char *a, const char *b) {
    return a && b && strcmp(a, b) == 0;
}

RP_UNUSED_FN long long rp_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (long long)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

RP_UNUSED_FN void rp_msleep(int ms) {
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

/* append one JSON line to <rundir>/events.jsonl — operational trace only */
RP_UNUSED_FN void rp_event(const char *rundir, const char *fmt, ...) {
    char path[1024];
    snprintf(path, sizeof path, "%s/events.jsonl", rundir);
    FILE *f = fopen(path, "a");
    if (!f) return;
    va_list ap;
    va_start(ap, fmt);
    vfprintf(f, fmt, ap);
    va_end(ap);
    fputc('\n', f);
    fclose(f);
}

RP_UNUSED_FN int rp_read_file(const char *path, char *buf, size_t cap) {
    FILE *f = fopen(path, "r");
    if (!f) return -1;
    size_t n = fread(buf, 1, cap - 1, f);
    fclose(f);
    buf[n] = 0;
    return (int)n;
}

/* atomic write: tmp + rename so readers never see a torn registry */
RP_UNUSED_FN int rp_write_file_atomic(const char *path, const char *data) {
    char tmp[1100];
    snprintf(tmp, sizeof tmp, "%s.tmp", path);
    FILE *f = fopen(tmp, "w");
    if (!f) return -1;
    fputs(data, f);
    fclose(f);
    return rename(tmp, path);
}

/* extract "name": <integer-or-"string"> from flat JSON; for evidence flow
 * only — never for trust decisions */
RP_UNUSED_FN int rp_json_field_ll(const char *json, const char *key,
                            long long *out) {
    char pat[80];
    snprintf(pat, sizeof pat, "\"%s\":", key);
    const char *p = strstr(json, pat);
    if (!p) return -1;
    p += strlen(pat);
    while (*p == ' ') p++;
    *out = strtoll(p, NULL, 10);
    return 0;
}

RP_UNUSED_FN int rp_json_field_str(const char *json, const char *key,
                             char *out, size_t cap) {
    char pat[80];
    snprintf(pat, sizeof pat, "\"%s\":", key);
    const char *p = strstr(json, pat);
    if (!p) return -1;
    p += strlen(pat);
    while (*p == ' ') p++;
    if (*p == '"') {
        p++;
        const char *e = strchr(p, '"');
        if (!e || (size_t)(e - p) >= cap) return -1;
        memcpy(out, p, (size_t)(e - p));
        out[e - p] = 0;
        return 0;
    }
    return -1;
}

#endif /* RP_COMMON_H */
