package gate

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestWaitReleaseFailsClosedOnTimeout(t *testing.T) {
	release := filepath.Join(t.TempDir(), "release")
	started := time.Now()
	err := WaitRelease(context.Background(), release, time.Second)
	if !errors.Is(err, ErrReleaseTimeout) {
		t.Fatalf("expected timeout, got %v", err)
	}
	if time.Since(started) < time.Second {
		t.Fatal("gate returned before fail-closed timeout")
	}
}
