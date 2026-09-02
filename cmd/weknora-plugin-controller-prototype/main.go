package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/network"
	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/prototype"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		printJSON(os.Stderr, map[string]any{"status": "error", "error": err.Error()})
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("command is required: spec, run, recover-stdin, or network-helper")
	}
	switch args[0] {
	case "spec":
		return specCommand(args[1:])
	case "run":
		return runCommand(args[1:])
	case "recover-stdin":
		return recoverCommand()
	case "network-helper":
		return networkHelperCommand(args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func specCommand(args []string) error {
	set := flag.NewFlagSet("spec", flag.ContinueOnError)
	runID := set.String("run-id", "", "unique prototype run ID")
	image := set.String("image", "", "scratch artifact image")
	appRoot := set.String("app-root", "", "root visible to Controller")
	hostRoot := set.String("host-root", "", "same root visible to Docker daemon")
	controllerHostPath := set.String("controller-host-path", "", "host path to controller binary")
	if err := set.Parse(args); err != nil {
		return err
	}
	if *runID == "" || *image == "" || *appRoot == "" || *hostRoot == "" || *controllerHostPath == "" {
		return errors.New("run-id, image, app-root, host-root, and controller-host-path are required")
	}
	appRootClean, hostRootClean := filepath.Clean(*appRoot), filepath.Clean(*hostRoot)
	mapping := prototype.PathMapping{AppRoot: appRootClean, HostRoot: hostRootClean}
	artifactApp := filepath.Join(appRootClean, "artifact")
	grantApp := filepath.Join(appRootClean, "allow", "source")
	runtimeApp := filepath.Join(appRootClean, "runtime", *runID, "datasource-local", "1")
	artifactHost, err := mapping.HostPath(artifactApp)
	if err != nil {
		return err
	}
	grantHost, err := mapping.HostPath(grantApp)
	if err != nil {
		return err
	}
	runtimeHost, err := mapping.HostPath(runtimeApp)
	if err != nil {
		return err
	}
	grant, err := prototype.CreateGrant(filepath.Join(appRootClean, "allow"), "source", 1)
	if err != nil {
		return err
	}
	spec := prototype.PrototypeInstanceSpec{
		RunID: *runID, PluginID: "local-directory-prototype", DataSourceID: "datasource-local", Generation: 1,
		Image: *image, ArtifactAppPath: artifactApp, ArtifactHostPath: artifactHost,
		GrantAppPath: grantApp, GrantHostPath: grantHost, RuntimeAppPath: runtimeApp, RuntimeHostPath: runtimeHost,
		ControllerAppPath: filepath.Clean(os.Args[0]), ControllerHostPath: filepath.Clean(*controllerHostPath),
		ArtifactMapping: mapping, GrantMapping: mapping, RuntimeMapping: mapping,
		Grant: grant, ContainerUser: "1000:1000", MemoryBytes: prototype.DefaultMemoryBytes,
		CPUQuota: prototype.DefaultCPUQuota, PidsLimit: prototype.DefaultPidsLimit, NetworkPolicy: "none",
		BPFPinRoot: filepath.Join("/sys/fs/bpf/weknora-plugin-prototype", *runID),
	}
	if err := spec.Validate(); err != nil {
		return err
	}
	return printJSON(os.Stdout, spec)
}

func recoverCommand() error {
	data, err := io.ReadAll(io.LimitReader(os.Stdin, 128*1024))
	if err != nil {
		return err
	}
	var spec prototype.PrototypeInstanceSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return err
	}
	controller, err := prototype.NewController()
	if err != nil {
		return err
	}
	defer controller.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	recovered, err := controller.Recover(ctx, spec)
	if err != nil {
		return err
	}
	return printJSON(os.Stdout, map[string]any{"recovered": recovered, "pid": os.Getpid()})
}

func runCommand(args []string) error {
	set := flag.NewFlagSet("run", flag.ContinueOnError)
	specPath := set.String("spec", "", "prototype spec JSON")
	if err := set.Parse(args); err != nil {
		return err
	}
	if *specPath == "" {
		return errors.New("spec path is required")
	}
	data, err := os.ReadFile(*specPath)
	if err != nil {
		return err
	}
	var spec prototype.PrototypeInstanceSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return err
	}
	controller, err := prototype.NewController()
	if err != nil {
		return err
	}
	defer controller.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	evidence, err := controller.Run(ctx, spec)
	if outputErr := printJSON(os.Stdout, evidence); outputErr != nil {
		return errors.Join(err, outputErr)
	}
	return err
}

func networkHelperCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("network helper operation is required")
	}
	operation := args[0]
	set := flag.NewFlagSet("network-helper "+operation, flag.ContinueOnError)
	containerID := set.String("container-id", "", "target Docker container ID")
	pinRoot := set.String("pin-root", "", "bpffs pin root")
	count := set.Int("count", 0, "audit event count")
	runID := set.String("run-id", "", "prototype run ID")
	pluginID := set.String("plugin-id", "", "plugin ID")
	dataSourceID := set.String("data-source-id", "", "data source ID")
	generation := set.Uint64("generation", 0, "generation")
	if err := set.Parse(args[1:]); err != nil {
		return err
	}
	if *containerID == "" || *pinRoot == "" {
		return errors.New("container-id and pin-root are required")
	}
	switch operation {
	case "attach":
		cgroupPath, err := network.FindDockerCgroup("/sys/fs/cgroup", *containerID)
		if err != nil {
			return err
		}
		cgroupID, err := network.CgroupID(cgroupPath)
		if err != nil {
			return err
		}
		policy, err := network.AttachAndPin(cgroupPath, *pinRoot)
		if err != nil {
			return err
		}
		if err := policy.CloseHandlesKeepPins(); err != nil {
			return err
		}
		return printJSON(os.Stdout, map[string]any{"status": "policy_ready", "cgroup_id": cgroupID})
	case "inspect":
		policy, err := network.OpenPinned(*pinRoot)
		if err != nil {
			return err
		}
		if err := policy.CloseHandlesKeepPins(); err != nil {
			return err
		}
		return printJSON(os.Stdout, map[string]string{"status": "policy_ready"})
	case "read":
		if *count < 1 || *runID == "" || *pluginID == "" || *dataSourceID == "" || *generation == 0 {
			return errors.New("read requires count and complete instance identity")
		}
		policy, err := network.OpenPinned(*pinRoot)
		if err != nil {
			return err
		}
		defer policy.CloseHandlesKeepPins()
		if err := printJSON(os.Stdout, map[string]string{"status": "audit_consumer_ready"}); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		events, err := policy.ReadEvents(ctx, *count, network.Identity{
			RunID: *runID, PluginID: *pluginID, DataSourceID: *dataSourceID, Generation: *generation,
		})
		if err != nil {
			return err
		}
		return printJSON(os.Stdout, events)
	case "detach":
		policy, err := network.OpenPinned(*pinRoot)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		return policy.Detach()
	default:
		return fmt.Errorf("unknown network helper operation %q", operation)
	}
}

func printJSON(file *os.File, value any) error {
	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(value)
}
