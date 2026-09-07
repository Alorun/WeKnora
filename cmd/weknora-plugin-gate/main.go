// The administrator-built gate is an entrypoint supervised by Docker init,
// not necessarily PID 1. No plugin code runs until the backend releases it.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Tencent/WeKnora/internal/plugin/control"
	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/gate"
)

func main() {
	entry := flag.String("entry", "", "package-relative executable")
	flag.Parse()
	if flag.NArg() != 0 || control.ValidateEntrypoint(*entry) != nil {
		os.Exit(64)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	data, err := os.ReadFile("/run/weknora/bootstrap.json")
	var boot struct{ ArtifactDevice, ArtifactInode, GrantDevice, GrantInode uint64 }
	if err != nil || len(data) > 4096 || json.Unmarshal(data, &boot) != nil {
		os.Exit(65)
	}
	for path, identity := range map[string][2]uint64{"/opt/weknora/artifact": {boot.ArtifactDevice, boot.ArtifactInode}, "/data/source": {boot.GrantDevice, boot.GrantInode}} {
		var st syscall.Stat_t
		if syscall.Stat(path, &st) != nil || uint64(st.Dev) != identity[0] || st.Ino != identity[1] {
			fmt.Fprintln(os.Stderr, "gate: mapped directory identity mismatch")
			os.Exit(65)
		}
	}
	if err := os.WriteFile("/run/weknora/gate.ready", []byte("mount-identities-verified"), 0600); err != nil {
		os.Exit(65)
	}
	if err := gate.WaitRelease(ctx, "/run/weknora/release", 30*time.Second); err != nil {
		fmt.Fprintln(os.Stderr, "gate: release failed")
		os.Exit(78)
	}
	path := "/opt/weknora/artifact/" + *entry
	// Bootstrap has identity and nonce only; configuration travels over UDS.
	if err := syscall.Exec(path, []string{path}, []string{"WEKNORA_PLUGIN_BOOTSTRAP=/run/weknora/bootstrap.json", "GOMAXPROCS=2"}); err != nil {
		fmt.Fprintln(os.Stderr, "gate: exec failed")
		os.Exit(71)
	}
}
