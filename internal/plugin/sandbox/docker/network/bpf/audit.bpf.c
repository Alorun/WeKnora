// SPDX-License-Identifier: GPL-2.0
#include <linux/bpf.h>
#include <linux/in.h>
#include <linux/in6.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

#define AF_INET 2
#define AF_INET6 10

struct audit_event {
    __u64 cgroup_id;
    __u64 ktime_ns;
    __u32 pid;
    __u32 uid;
    __u16 destination_port;
    __u8 family;
    __u8 protocol;
    __u8 hook;
    __u8 padding[3];
    __u8 destination_address[16];
};

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 1 << 20);
} audit_events SEC(".maps");

// Loss is observable even if a plugin deliberately floods the bounded ring.
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u64);
} audit_dropped SEC(".maps");

enum audit_hook {
    HOOK_CONNECT4 = 1,
    HOOK_CONNECT6 = 2,
    HOOK_SENDMSG4 = 3,
    HOOK_SENDMSG6 = 4,
};

static __always_inline int deny_and_audit(struct bpf_sock_addr *ctx, __u8 family, __u8 hook)
{
    struct audit_event event = {};
    __u64 pid_tgid = bpf_get_current_pid_tgid();
    __u64 uid_gid = bpf_get_current_uid_gid();

    event.cgroup_id = bpf_get_current_cgroup_id();
    event.ktime_ns = bpf_ktime_get_ns();
    event.pid = pid_tgid >> 32;
    event.uid = (__u32)uid_gid;
    event.destination_port = bpf_ntohs((__u16)ctx->user_port);
    event.family = family;
    event.protocol = ctx->protocol;
    event.hook = hook;

    if (family == AF_INET) {
        __builtin_memcpy(event.destination_address, &ctx->user_ip4, sizeof(ctx->user_ip4));
    } else {
        __builtin_memcpy(event.destination_address, ctx->user_ip6, sizeof(ctx->user_ip6));
    }

    if (bpf_ringbuf_output(&audit_events, &event, sizeof(event), 0) < 0) {
        __u32 key = 0;
        __u64 *dropped = bpf_map_lookup_elem(&audit_dropped, &key);
        if (dropped) __sync_fetch_and_add(dropped, 1);
    }
    return 0;
}

SEC("cgroup/connect4")
int deny_connect4(struct bpf_sock_addr *ctx)
{
    return deny_and_audit(ctx, AF_INET, HOOK_CONNECT4);
}

SEC("cgroup/connect6")
int deny_connect6(struct bpf_sock_addr *ctx)
{
    return deny_and_audit(ctx, AF_INET6, HOOK_CONNECT6);
}

SEC("cgroup/sendmsg4")
int deny_sendmsg4(struct bpf_sock_addr *ctx)
{
    return deny_and_audit(ctx, AF_INET, HOOK_SENDMSG4);
}

SEC("cgroup/sendmsg6")
int deny_sendmsg6(struct bpf_sock_addr *ctx)
{
    return deny_and_audit(ctx, AF_INET6, HOOK_SENDMSG6);
}

char LICENSE[] SEC("license") = "GPL";
