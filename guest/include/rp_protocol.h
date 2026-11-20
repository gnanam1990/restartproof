/* RestartProof — wire protocol shared by the guest harness.
 *
 * Two transports carry the SAME message layout:
 *   - worker endpoint: QNX native IPC (name_attach/name_open + MsgSend)
 *     on __QNXNTO__, or Unix-domain sockets on POSIX hosts.
 *   - control endpoint: TCP on every platform (the host runner must reach
 *     the guest-side supervisor; framed as [u32 len][struct]).
 *
 * All message structs are packed. Integer fields are little-endian on the
 * wire; supported targets (x86_64, aarch64 hosts and QNX guests) are all
 * little-endian.
 */
#ifndef RP_PROTOCOL_H
#define RP_PROTOCOL_H

#include <stdint.h>

#pragma pack(push, 1)

#define RP_PROTOCOL_VERSION 1u

#define RP_ID_LEN     64
#define RP_NONCE_LEN  32
#define RP_DIGEST_LEN 64
#define RP_PAYLOAD_LEN 512
#define RP_SINKID_LEN 64
#define RP_DETAIL_LEN 160

/* client -> worker: one delivery of a logical operation.
 * hold_at_barrier != 0 asks the cooperating test worker to hold after the
 * sink commit but before the reply — the point where the restart lands. */
typedef struct {
    uint32_t protocol_version;
    uint32_t hold_at_barrier;
    char     run_id[RP_ID_LEN];
    char     run_nonce[RP_NONCE_LEN];
    char     op_id[RP_ID_LEN];
    char     attempt_id[RP_ID_LEN];
    char     payload_digest[RP_DIGEST_LEN + 1];
    char     payload_json[RP_PAYLOAD_LEN]; /* verbatim JSON, NUL-terminated */
} rp_msg_deliver_t;

/* worker -> client reply. status 0 = delivered; negative = sink rejected. */
typedef struct {
    uint32_t protocol_version;
    int32_t  status;
    int64_t  receipt_seq;
    char     sink_id[RP_SINKID_LEN];
    char     detail[RP_DETAIL_LEN];
} rp_msg_reply_t;

enum {
    RP_CTL_BARRIER  = 1, /* worker -> supervisor: held post-commit */
    RP_CTL_RESTART  = 2, /* runner -> supervisor: stop+respawn worker */
    RP_CTL_CONTINUE = 3, /* supervisor -> held worker: reply now */
    RP_CTL_STOP     = 4, /* runner -> supervisor: end the scenario */
    RP_CTL_STATUS   = 5  /* runner -> supervisor: liveness query */
};

typedef struct {
    uint32_t protocol_version;
    uint32_t kind;
    uint32_t generation;          /* worker generation the msg concerns */
    uint32_t control_generation;  /* runner journal generation (stale check) */
    int64_t  receipt_seq;
    char     run_nonce[RP_NONCE_LEN];
    char     op_id[RP_ID_LEN];
    char     attempt_id[RP_ID_LEN];
} rp_msg_ctl_t;

enum {
    RP_CTL_OK            = 0,
    RP_CTL_ERR_NONCE     = -1,
    RP_CTL_ERR_STALE     = -2,
    RP_CTL_ERR_NO_BARRIER= -3,
    RP_CTL_ERR_BAD_MSG   = -4,
    RP_CTL_ERR_INTERNAL  = -5
};

typedef struct {
    uint32_t protocol_version;
    int32_t  status;
    uint32_t generation;
    char     detail[RP_DETAIL_LEN];
} rp_msg_ctl_reply_t;

#pragma pack(pop)

/* result codes recorded by the client (mirrored in the JSON report) */
enum {
    RP_SEND_OK             = 0,
    RP_SEND_LOST_REPLY     = 1, /* peer vanished mid-request (ESRCH class) */
    RP_SEND_TIMEDOUT       = 2,
    RP_SEND_CONNECT_FAILED = 3,
    RP_SEND_NO_ENDPOINT    = 4
};

#endif /* RP_PROTOCOL_H */
