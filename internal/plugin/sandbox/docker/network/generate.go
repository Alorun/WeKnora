package network

// The checked-in object is generated for Linux amd64 by the stage-1 prototype.
// It is intentionally not a public plugin ABI.
//go:generate clang -O2 -g -target bpf -D__TARGET_ARCH_x86 -c bpf/audit.bpf.c -o audit_bpf.o
//go:generate llvm-strip -g audit_bpf.o
