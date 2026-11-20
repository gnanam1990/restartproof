/* Control channel: TCP on every platform (POSIX hosts and QNX), framed
 * [u32 len][struct]. The host runner reaches the guest supervisor here;
 * the worker reaches it for barrier notification. */
#include "rp_transport.h"
#include "rp_common.h"

#include <poll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static int wait_io(int fd, short events, int timeout_ms) {
    struct pollfd p = { fd, events, 0 };
    int r;
    do { r = poll(&p, 1, timeout_ms); } while (r < 0 && errno == EINTR);
    if (r <= 0) return r;
    if (p.revents & events) return 1; /* data ready beats HUP */
    return (p.revents & (POLLERR | POLLHUP)) ? -2 : 1;
}

static int read_full(int fd, void *buf, size_t len) {
    char *p = buf;
    while (len) {
        ssize_t n = read(fd, p, len);
        if (n == 0) return 0;
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

static int ctl_frame_write(int fd, const void *msg, uint32_t len) {
    uint32_t hdr = len;
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    hdr = __builtin_bswap32(hdr);
#endif
    if (write_full(fd, &hdr, sizeof hdr) < 0) return -1;
    return write_full(fd, msg, len);
}

/* Returns 0 on a complete frame, 1 on timeout, <0 on error/EOF. */
static int ctl_frame_read(int fd, void *msg, uint32_t cap,
                          int timeout_ms) {
    int r = wait_io(fd, POLLIN, timeout_ms);
    if (r <= 0) return r == 0 ? 1 : r;   /* timeout gets its own code */
    uint32_t hdr;
    int rr = read_full(fd, &hdr, sizeof hdr);
    if (rr <= 0) return -2;
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    hdr = __builtin_bswap32(hdr);
#endif
    if (hdr > cap) return -3;
    rr = read_full(fd, msg, hdr);
    return rr <= 0 ? -2 : 0;
}

int rp_ctl_listen(int port, int *out_fd) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    a.sin_port = htons((uint16_t)port);
    if (bind(fd, (struct sockaddr *)&a, sizeof a) < 0 ||
        listen(fd, 8) < 0) { close(fd); return -1; }
    socklen_t alen = sizeof a;
    getsockname(fd, (struct sockaddr *)&a, &alen);
    *out_fd = fd;
    return ntohs(a.sin_port);
}

int rp_ctl_accept(int listen_fd, int *out_fd, int timeout_ms) {
    /* 0 accepted, 1 timeout, <0 error */
    int r = wait_io(listen_fd, POLLIN, timeout_ms);
    if (r <= 0) return r == 0 ? 1 : r;
    int fd = accept(listen_fd, NULL, NULL);
    if (fd < 0) return -1;
    *out_fd = fd;
    return 0;
}

int rp_ctl_send_recv(int fd, const rp_msg_ctl_t *req,
                     rp_msg_ctl_reply_t *resp, int timeout_ms) {
    if (ctl_frame_write(fd, req, sizeof *req) < 0) return -1;
    return ctl_frame_read(fd, resp, sizeof *resp, timeout_ms);
}

int rp_ctl_recv(int fd, rp_msg_ctl_t *out, int timeout_ms) {
    return ctl_frame_read(fd, out, sizeof *out, timeout_ms);
}

int rp_ctl_recv_reply(int fd, rp_msg_ctl_reply_t *out, int timeout_ms) {
    return ctl_frame_read(fd, out, sizeof *out, timeout_ms);
}

int rp_ctl_send(int fd, const rp_msg_ctl_t *req) {
    return ctl_frame_write(fd, req, sizeof *req);
}

int rp_ctl_reply(int fd, const rp_msg_ctl_reply_t *resp) {
    return ctl_frame_write(fd, resp, sizeof *resp);
}

int rp_ctl_connect(const char *host, int port, int *out_fd) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &a.sin_addr) != 1) { close(fd); return -1; }
    if (connect(fd, (struct sockaddr *)&a, sizeof a) < 0) {
        close(fd); return -1;
    }
    *out_fd = fd;
    return 0;
}
