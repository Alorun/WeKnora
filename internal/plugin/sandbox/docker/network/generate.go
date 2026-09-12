package network

// The checked-in object is generated for the Linux amd64 plugin backend.
// It is intentionally not a public plugin ABI.
//go:generate clang -O2 -g -target bpf -D__TARGET_ARCH_x86 -c bpf/audit.bpf.c -o audit_bpf.o
//go:generate llvm-strip -g audit_bpf.o
