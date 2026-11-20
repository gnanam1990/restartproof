/* Transport abstraction for the measured client<->worker channel.
 *
 * QNX (__QNXNTO__): name_attach/name_open + MsgSend/MsgReceive/MsgReply.
 * POSIX host:     Unix-domain SOCK_STREAM in the run directory.
 * The semantics are the synchronous ones the product measures: a blocked
 * sender whose server dies observes a lost reply, never a guessed outcome.
 */
#ifndef RP_TRANSPORT_H
#define RP_TRANSPORT_H

#include "rp_protocol.h"

typedef struct rp_listener rp_listener_t;
typedef struct rp_conn rp_conn_t;

/* server side */
int  rp_listen(const char *endpoint, rp_listener_t **out);
/* accept + read one request; returns 0 with *cli set to a reply token */
int  rp_listen_recv(rp_listener_t *l, rp_msg_deliver_t *out, rp_conn_t **cli,
                    int timeout_ms);
int  rp_conn_reply(rp_conn_t *c, const rp_msg_reply_t *msg);
void rp_conn_close(rp_conn_t *c);
void rp_unlisten(rp_listener_t *l);

/* client side */
int  rp_connect(const char *endpoint, rp_conn_t **out);
/* synchronous request with a bounded wait; returns RP_SEND_* */
int  rp_request(rp_conn_t *c, const rp_msg_deliver_t *req,
                rp_msg_reply_t *resp, int timeout_ms);
void rp_disconnect(rp_conn_t *c);

/* control channel (TCP everywhere; framed [u32 len][struct]) */
int  rp_ctl_listen(int port, int *out_fd);
int  rp_ctl_accept(int listen_fd, int *out_fd, int timeout_ms);
int  rp_ctl_send_recv(int fd, const rp_msg_ctl_t *req,
                      rp_msg_ctl_reply_t *resp, int timeout_ms);
int  rp_ctl_recv(int fd, rp_msg_ctl_t *out, int timeout_ms);
int  rp_ctl_recv_reply(int fd, rp_msg_ctl_reply_t *out, int timeout_ms);
int  rp_ctl_send(int fd, const rp_msg_ctl_t *req);
int  rp_ctl_reply(int fd, const rp_msg_ctl_reply_t *resp);
int  rp_ctl_connect(const char *host, int port, int *out_fd);

#endif /* RP_TRANSPORT_H */
