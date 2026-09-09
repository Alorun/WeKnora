package network

import (
	"bytes"

	"github.com/cilium/ebpf"
)

// CheckLoad exercises the same immutable object/verifier as AttachAndPin,
// without applying policy to any cgroup. All kernel handles are closed.
func CheckLoad() error {
	spec, err := ebpf.LoadCollectionSpecFromReader(bytes.NewReader(auditObject))
	if err != nil {
		return err
	}
	collection, err := ebpf.NewCollection(spec)
	if err != nil {
		return err
	}
	collection.Close()
	return nil
}
