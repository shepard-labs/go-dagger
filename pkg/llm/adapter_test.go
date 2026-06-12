package llm

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/shepard-labs/go-ai-sdk/anthropic"
)

func TestREQADAPTER001_GoAISDKImportOnlyInAdapter(t *testing.T) {
	root := filepath.Join("..", "..")
	var offenders []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), "github.com/shepard-labs/go-ai-sdk") && filepath.ToSlash(path) != "../../pkg/llm/adapter.go" {
			offenders = append(offenders, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk repo: %v", err)
	}
	if len(offenders) > 0 {
		t.Fatalf("production go-ai-sdk imports outside adapter.go: %v", offenders)
	}
}

type fakeAnthropicModel struct {
	lastOptions anthropic.GenerateOptions
	result      *anthropic.GenerateResult
	err         error
}

func (f *fakeAnthropicModel) ModelID() string                          { return "fake" }
func (f *fakeAnthropicModel) Provider() string                         { return "fake" }
func (f *fakeAnthropicModel) SupportURLs() map[string][]*regexp.Regexp { return nil }
func (f *fakeAnthropicModel) DoGenerate(ctx context.Context, opts anthropic.GenerateOptions) (*anthropic.GenerateResult, error) {
	f.lastOptions = opts
	return f.result, f.err
}
func (f *fakeAnthropicModel) DoStream(ctx context.Context, opts anthropic.StreamOptions) (*anthropic.StreamResult, error) {
	return nil, nil
}

func TestREQADAPTER002_APICallError429529Unwrapped(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, 529} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			want := &anthropic.APICallError{Status: status, Message: "retry"}
			client := NewAnthropicAdapter(&fakeAnthropicModel{err: want})
			_, err := client.Generate(context.Background(), GenerateOptions{})
			var got *anthropic.APICallError
			if !errors.As(err, &got) || got != want {
				t.Fatalf("error = %v, want original APICallError", err)
			}
		})
	}
}

func TestREQADAPTER003_UnknownFinishReasonMapsToError(t *testing.T) {
	model := &fakeAnthropicModel{result: &anthropic.GenerateResult{FinishReason: anthropic.FinishReason("new")}}
	result, err := NewAnthropicAdapter(model).Generate(context.Background(), GenerateOptions{})
	if err != nil {
		t.Fatalf("Generate error = %v", err)
	}
	if result.FinishReason != FinishReasonError {
		t.Fatalf("finish = %q, want %q", result.FinishReason, FinishReasonError)
	}
}

func TestREQADAPTER003_UnknownRoleReturnsError(t *testing.T) {
	_, err := NewAnthropicAdapter(&fakeAnthropicModel{}).Generate(context.Background(), GenerateOptions{Messages: []Message{{Role: "system"}}})
	if err == nil {
		t.Fatal("Generate error = nil, want unknown role error")
	}
}

func TestREQADAPTER004_GeneratorFuncSatisfiesClient(t *testing.T) {
	var _ Client = GeneratorFunc(func(context.Context, GenerateOptions) (*GenerateResult, error) { return nil, nil })
}

func TestREQADAPTER_SystemMessageConversion(t *testing.T) {
	model := &fakeAnthropicModel{result: &anthropic.GenerateResult{FinishReason: anthropic.FinishReasonStop}}
	_, err := NewAnthropicAdapter(model).Generate(context.Background(), GenerateOptions{System: "system"})
	if err != nil {
		t.Fatalf("Generate error = %v", err)
	}
	message, ok := model.lastOptions.Messages[0].(anthropic.SystemMessage)
	if !ok || message.Content != "system" {
		t.Fatalf("first message = %#v, want SystemMessage", model.lastOptions.Messages[0])
	}
}

func TestREQADAPTER_UserAndAssistantMessageConversion(t *testing.T) {
	model := &fakeAnthropicModel{result: &anthropic.GenerateResult{FinishReason: anthropic.FinishReasonStop}}
	_, err := NewAnthropicAdapter(model).Generate(context.Background(), GenerateOptions{Messages: []Message{
		{Role: "user", Content: []Content{TextContent{Text: "hello"}}},
		{Role: "assistant", Content: []Content{TextContent{Text: "hi"}}},
	}})
	if err != nil {
		t.Fatalf("Generate error = %v", err)
	}
	if _, ok := model.lastOptions.Messages[0].(anthropic.UserMessage); !ok {
		t.Fatalf("message 0 = %#v, want UserMessage", model.lastOptions.Messages[0])
	}
	if _, ok := model.lastOptions.Messages[1].(anthropic.AssistantMessage); !ok {
		t.Fatalf("message 1 = %#v, want AssistantMessage", model.lastOptions.Messages[1])
	}
}

func TestREQADAPTER_ToolUseAndToolResultConversion(t *testing.T) {
	model := &fakeAnthropicModel{result: &anthropic.GenerateResult{FinishReason: anthropic.FinishReasonToolCalls, Content: []anthropic.Content{
		anthropic.TextContent{Text: "use"},
		anthropic.ToolCallContent{ToolCallID: "call", ToolName: "tool", Input: []byte(`{"x":1}`)},
	}}}
	result, err := NewAnthropicAdapter(model).Generate(context.Background(), GenerateOptions{Messages: []Message{
		{Role: "assistant", Content: []Content{ToolUseContent{ID: "call", Name: "tool", Input: []byte(`{"x":1}`)}}},
		{Role: "user", Content: []Content{ToolResultContent{ToolUseID: "call", Text: "ok", IsError: true}}},
	}})
	if err != nil {
		t.Fatalf("Generate error = %v", err)
	}
	assistant := model.lastOptions.Messages[0].(anthropic.AssistantMessage)
	if tool, ok := assistant.Content[0].(anthropic.ToolCallContent); !ok || tool.ToolCallID != "call" || tool.ToolName != "tool" {
		t.Fatalf("assistant content = %#v, want ToolCallContent", assistant.Content[0])
	}
	user := model.lastOptions.Messages[1].(anthropic.UserMessage)
	if tool, ok := user.Content[0].(anthropic.ToolResultContent); !ok || tool.ToolCallID != "call" || !tool.IsError {
		t.Fatalf("user content = %#v, want ToolResultContent", user.Content[0])
	}
	if len(result.Content) != 2 || result.FinishReason != FinishReasonToolCalls {
		t.Fatalf("result = %#v", result)
	}
}

func TestREQADAPTER_ToolSchemaConversion(t *testing.T) {
	model := &fakeAnthropicModel{result: &anthropic.GenerateResult{FinishReason: anthropic.FinishReasonStop}}
	_, err := NewAnthropicAdapter(model).Generate(context.Background(), GenerateOptions{Tools: []Tool{{Name: "tool", Description: "desc", InputSchema: []byte(`{"type":"object"}`)}}})
	if err != nil {
		t.Fatalf("Generate error = %v", err)
	}
	schema, ok := model.lastOptions.Tools[0].InputSchema.(map[string]any)
	if !ok || schema["type"] != "object" {
		t.Fatalf("schema = %#v, want decoded map", model.lastOptions.Tools[0].InputSchema)
	}
}

func TestREQADAPTER_UsageConversion(t *testing.T) {
	model := &fakeAnthropicModel{result: &anthropic.GenerateResult{FinishReason: anthropic.FinishReasonStop, Usage: anthropic.Usage{InputTokens: anthropic.TokenUsage{Total: 7}, OutputTokens: anthropic.TokenUsage{Total: 11}}}}
	result, err := NewAnthropicAdapter(model).Generate(context.Background(), GenerateOptions{})
	if err != nil {
		t.Fatalf("Generate error = %v", err)
	}
	if result.Usage.InputTokens != 7 || result.Usage.OutputTokens != 11 {
		t.Fatalf("usage = %#v, want 7/11", result.Usage)
	}
}
