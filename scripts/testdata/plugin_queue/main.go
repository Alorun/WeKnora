// Test-only administration of the isolated C3 Redis, never part of server.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/hibiken/asynq"
)

// Existing QueueDefault / TypeDocumentProcess wire names. Avoid importing the
// application's CGo-dependent model graph into this tiny static test utility.
const documentQueue = "default"

func main() {
	port, err := strconv.Atoi(os.Getenv("C3_REDIS_PORT"))
	if err != nil || port < 1024 || port > 65535 || os.Getenv("C3_DEPLOYMENT") == "" || len(os.Args) < 2 {
		panic("explicit isolated C3 Redis port/deployment/action required")
	}
	if os.Args[1] == "check-pins" {
		_, err := os.Stat(filepath.Join("/sys/fs/bpf/weknora-plugin", os.Getenv("C3_DEPLOYMENT")))
		if !os.IsNotExist(err) {
			panic(fmt.Sprintf("deployment BPF pins remain or cannot inspect: %v", err))
		}
		fmt.Println("PASS no deployment BPF pin/map/link remains")
		return
	}
	i := asynq.NewInspector(asynq.RedisClientOpt{Addr: fmt.Sprintf("127.0.0.1:%d", port)})
	defer i.Close()
	switch os.Args[1] {
	case "pause":
		err = i.PauseQueue(documentQueue)
	case "resume":
		err = i.UnpauseQueue(documentQueue)
	case "drop":
		if len(os.Args) != 3 {
			panic("knowledge ID required")
		}
		var tasks []*asynq.TaskInfo
		tasks, err = i.ListPendingTasks(documentQueue, asynq.PageSize(100))
		removed := 0
		for _, task := range tasks {
			var payload struct {
				KnowledgeID string `json:"knowledge_id"`
			}
			if task.Type == "document:process" && json.Unmarshal(task.Payload, &payload) == nil && payload.KnowledgeID == os.Args[2] {
				if err = i.DeleteTask(documentQueue, task.ID); err != nil {
					break
				}
				removed++
			}
		}
		if err == nil && removed != 1 {
			err = fmt.Errorf("expected exactly one lost pending document task, got %d", removed)
		}
	default:
		err = fmt.Errorf("unknown action")
	}
	if err != nil {
		panic(err)
	}
}
