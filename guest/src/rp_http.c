#include "rp_http.h"
#include "rp_common.h"

#include <poll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* connect with a bounded wait */
static int tcp_connect(const char *host, int port, int timeout_ms) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &a.sin_addr) != 1) { close(fd); return -1; }
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    int r = connect(fd, (struct sockaddr *)&a, sizeof a);
    if (r < 0 && errno == EINPROGRESS) {
        struct pollfd p = { fd, POLLOUT, 0 };
        r = poll(&p, 1, timeout_ms);
        if (r <= 0) { close(fd); return -1; }
        int err = 0; socklen_t el = sizeof err;
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &el);
        if (err) { close(fd); errno = err; return -1; }
    } else if (r < 0) { close(fd); return -1; }
    fcntl(fd, F_SETFL, flags);
    return fd;
}

static int wait_readable(int fd, int timeout_ms) {
    struct pollfd p = { fd, POLLIN, 0 };
    int r;
    do { r = poll(&p, 1, timeout_ms); } while (r < 0 && errno == EINTR);
    return r;
}

int rp_http_post(const char *url, const char *bearer, const char *body,
                 int timeout_ms, int *status, char *resp, int cap) {
    char host[128] = {0}, path[512] = {0};
    int port = 80;
    if (sscanf(url, "http://%127[^:/]:%d%511s", host, &port, path) < 2) {
        if (sscanf(url, "http://%127[^/]%511s", host, path) < 1)
            return -1;
    }
    if (!path[0]) strcpy(path, "/");
    int fd = tcp_connect(host, port, timeout_ms);
    if (fd < 0) return -2;

    char req[2048];
    int n = snprintf(req, sizeof req,
        "POST %s HTTP/1.0\r\nHost: %s\r\nContent-Type: application/json\r\n"
        "Authorization: Bearer %s\r\nContent-Length: %zu\r\n\r\n%s",
        path, host, bearer ? bearer : "", strlen(body), body);
    if (n <= 0 || n >= (int)sizeof req) { close(fd); return -3; }

    size_t off = 0;
    while (off < (size_t)n) {
        ssize_t w = write(fd, req + off, (size_t)n - off);
        if (w < 0) { if (errno == EINTR) continue; close(fd); return -4; }
        off += (size_t)w;
    }
    int total = 0;
    long long deadline = rp_now_ms() + timeout_ms;
    while (total < cap - 1) {
        int remain = (int)(deadline - rp_now_ms());
        if (remain <= 0) { close(fd); return -5; }
        if (wait_readable(fd, remain) <= 0) { close(fd); return -5; }
        ssize_t r = read(fd, resp + total, (size_t)(cap - 1 - total));
        if (r == 0) break;
        if (r < 0) { if (errno == EINTR) continue; close(fd); return -6; }
        total += (int)r;
    }
    resp[total] = 0;
    close(fd);
    if (sscanf(resp, "HTTP/%*s %d", status) != 1) return -7;
    char *body_start = strstr(resp, "\r\n\r\n");
    if (body_start) memmove(resp, body_start + 4, strlen(body_start + 4) + 1);
    return 0;
}
