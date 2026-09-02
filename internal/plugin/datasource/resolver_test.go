package datasource

import (
	"errors"
	"testing"
)

type testHandle string

func (h testHandle) InstanceID() string { return string(h) }

func TestResolverRejectsOldGenerationAndUnpublishes(t *testing.T) {
	resolver := NewResolver()
	if err := resolver.Publish("ds-1", 2, testHandle("instance-2")); err != nil {
		t.Fatal(err)
	}
	if err := resolver.Publish("ds-1", 1, testHandle("instance-1")); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("stale publish error = %v", err)
	}
	if err := resolver.Unpublish("ds-1", 1); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("stale unpublish error = %v", err)
	}
	resolved, err := resolver.Resolve("ds-1")
	if err != nil || resolved.Handle.InstanceID() != "instance-2" {
		t.Fatalf("resolved = %#v, err = %v", resolved, err)
	}
	if err := resolver.Unpublish("ds-1", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Resolve("ds-1"); !errors.Is(err, ErrHandleNotFound) {
		t.Fatalf("resolve after unpublish error = %v", err)
	}
}
