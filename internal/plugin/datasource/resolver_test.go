package datasource

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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

func TestResolverStopMatchesInstanceAndDrains(t *testing.T) {
	r := NewResolver()
	require.NoError(t, r.Publish("ds", 1, testHandle("old")))
	lease, err := r.Acquire("ds")
	require.NoError(t, err)
	drained, err := r.UnpublishInstance("ds", 1, "old")
	require.NoError(t, err)
	select {
	case <-drained:
		t.Fatal("lease has not drained")
	default:
	}
	require.NoError(t, r.Publish("ds", 1, testHandle("replacement")))
	_, err = r.UnpublishInstance("ds", 1, "old")
	require.NoError(t, err)
	current, err := r.Resolve("ds")
	require.NoError(t, err)
	require.Equal(t, "replacement", current.Handle.InstanceID())
	lease.Release()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case <-drained:
	case <-ctx.Done():
		t.Fatal("old lease did not drain")
	}
}
