package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
)

func (s *knowledgeService) BuildDocumentProcessTask(
	ctx context.Context, knowledgeID string,
) (*asynq.Task, []asynq.Option, error) {
	knowledge, err := s.repo.GetKnowledgeByIDOnly(ctx, knowledgeID)
	if err != nil {
		return nil, nil, err
	}
	kb, err := s.kbService.GetKnowledgeBaseByID(ctx, knowledge.KnowledgeBaseID)
	if err != nil {
		return nil, nil, err
	}
	overrides, _ := knowledge.ProcessOverrides()
	effective := ResolveProcessConfig(kb, overrides)
	questionCount := effective.QuestionGenerationConfig.QuestionCount
	if questionCount <= 0 {
		questionCount = 3
	}
	payload := types.DocumentProcessPayload{
		TenantID: knowledge.TenantID, KnowledgeID: knowledge.ID, KnowledgeBaseID: knowledge.KnowledgeBaseID,
		FilePath: knowledge.FilePath, FileName: knowledge.FileName, FileType: knowledge.FileType,
		EnableMultimodel:         effective.EnableMultimodel,
		EnableQuestionGeneration: effective.QuestionGenerationConfig.Enabled,
		QuestionCount:            questionCount, Language: types.LanguageFromContextOrDefault(ctx),
	}
	switch {
	case knowledge.Type == "url":
		payload.URL = knowledge.Source
	case knowledge.FilePath == "" && knowledge.Source != "":
		payload.FileURL = knowledge.Source
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal document process payload: %w", err)
	}
	return asynq.NewTask(types.TypeDocumentProcess, data), documentProcessTaskOptions(s.config), nil
}

func documentProcessTaskOptions(cfg *config.Config, extra ...asynq.Option) []asynq.Option {
	opts := []asynq.Option{
		asynq.Queue(types.QueueDefault),
		asynq.Timeout(config.DocumentProcessTimeout(cfg)),
		asynq.MaxRetry(3),
	}
	opts = append(opts, extra...)
	return opts
}

func knowledgePostProcessTaskOptions() []asynq.Option {
	return []asynq.Option{
		asynq.Queue(types.QueuePostProcess),
		asynq.MaxRetry(3),
		asynq.Timeout(30 * time.Minute),
	}
}
