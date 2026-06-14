package task

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/shepard-labs/go-ai-sdk/llm"
)

func TestToolRegistryImplementsLLMToolDispatcher(t *testing.T) {
	var _ llm.ToolDispatcher = ToolRegistry{}
}

func TestToolRegistryAgentLoopSurfacesMissingToolAsError(t *testing.T) {
	var call int
	client := llm.GeneratorFunc(func(context.Context, llm.GenerateOptions) (*llm.GenerateResult, error) {
		call++
		if call == 1 {
			return &llm.GenerateResult{
				FinishReason: llm.FinishReasonToolCalls,
				Content:      []llm.Content{llm.ToolUseContent{ID: "1", Name: "missing"}},
			}, nil
		}
		return &llm.GenerateResult{FinishReason: llm.FinishReasonStop}, nil
	})
	messages, _, err := llm.AgentLoop(context.Background(), client, llm.GenerateOptions{}, ToolRegistry{}, "", 3)
	if err != nil {
		t.Fatalf("AgentLoop error = %v", err)
	}
	if len(messages) < 2 {
		t.Fatalf("messages = %#v, want tool result message", messages)
	}
	result, ok := messages[1].Content[0].(llm.ToolResultContent)
	if !ok || !result.IsError {
		t.Fatalf("tool result = %#v, want IsError", messages[1].Content[0])
	}
}

func TestToolRegistryDispatchOK(t *testing.T) {
	want := json.RawMessage(`{"ok":true}`)
	registry := ToolRegistry{"ok": {Name: "ok", Handler: func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return want, nil
	}}}
	got, err := registry.Dispatch(context.Background(), "ok", nil)
	if err != nil {
		t.Fatalf("Dispatch error = %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("got = %s, want %s", got, want)
	}
}
