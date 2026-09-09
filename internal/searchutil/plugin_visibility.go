package searchutil

import "github.com/Tencent/WeKnora/internal/types"

// PluginKnowledgeVisible fences already indexed hits while the ledger has not
// activated their Knowledge (or has superseded it). Ordinary uploads preserve
// their existing retrieval semantics. Both text and graph hydration use it.
func PluginKnowledgeVisible(k *types.Knowledge) bool {
	if k == nil {
		return false
	}
	state := k.GetMetadata()["plugin_revision_state"]
	return state == "" || state == "active" && k.EnableStatus == "enabled"
}
