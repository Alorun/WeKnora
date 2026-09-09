package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

func TestPluginIndexedHitsHiddenUntilLedgerActivation(t *testing.T) {
	s := &knowledgeBaseService{}
	chunk := &types.Chunk{ID: "chunk", KnowledgeID: "knowledge", ChunkType: types.ChunkTypeText, IndexStatus: "ready", IsEnabled: true}
	hits := []*types.IndexWithScore{{ChunkID: chunk.ID, KnowledgeID: chunk.KnowledgeID}}
	knowledge := &types.Knowledge{ID: chunk.KnowledgeID, EnableStatus: "disabled", Metadata: types.JSON(`{"plugin_revision_state":"pending"}`)}
	for _, enrich := range []bool{false, true} {
		idx := s.buildChunkIndex(hits)
		got := s.assembleSearchResults(context.Background(), hits, map[string]*types.Chunk{chunk.ID: chunk}, map[string]*types.Knowledge{knowledge.ID: knowledge}, idx, enrich)
		if len(got) != 0 {
			t.Fatal("pending indexed plugin Knowledge escaped retrieval fence")
		}
	}
	knowledge.Metadata = types.JSON(`{"plugin_revision_state":"active"}`)
	knowledge.EnableStatus = "enabled"
	if got := s.assembleSearchResults(context.Background(), hits, map[string]*types.Chunk{chunk.ID: chunk}, map[string]*types.Knowledge{knowledge.ID: knowledge}, s.buildChunkIndex(hits), true); len(got) != 1 {
		t.Fatal("activated plugin Knowledge must remain searchable")
	}
	knowledge.EnableStatus = "disabled" // old active awaiting physical cleanup
	if got := s.assembleSearchResults(context.Background(), hits, map[string]*types.Chunk{chunk.ID: chunk}, map[string]*types.Knowledge{knowledge.ID: knowledge}, s.buildChunkIndex(hits), true); len(got) != 0 {
		t.Fatal("superseded index hit must stay hidden before cleanup")
	}
}

func TestIsSearchableChunkSkipsUnsynchronizedEdits(t *testing.T) {
	service := &knowledgeBaseService{}
	for _, status := range []string{"processing", "failed"} {
		chunk := &types.Chunk{ChunkType: types.ChunkTypeText, IndexStatus: status, IsEnabled: true}
		if service.isSearchableChunk(chunk) {
			t.Fatalf("chunk with index status %q should not be searchable", status)
		}
	}
	for _, status := range []string{"", "ready"} {
		chunk := &types.Chunk{ChunkType: types.ChunkTypeText, IndexStatus: status, IsEnabled: true}
		if !service.isSearchableChunk(chunk) {
			t.Fatalf("chunk with index status %q should be searchable", status)
		}
	}
}

func TestIsSearchableChunkSkipsDisabledChunk(t *testing.T) {
	service := &knowledgeBaseService{}
	chunk := &types.Chunk{
		ChunkType:   types.ChunkTypeFAQ,
		IndexStatus: "ready",
		IsEnabled:   false,
	}
	if service.isSearchableChunk(chunk) {
		t.Fatal("disabled FAQ chunk should never be searchable")
	}
}
