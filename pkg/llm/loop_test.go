package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

type mapDispatcher struct {
	mu       sync.Mutex
	calls    []string
	handlers map[string]func(context.Context, json.RawMessage) (json.RawMessage, error)
}

type loopClientFunc func(context.Context, GenerateOptions) (*GenerateResult, error)

func (f loopClientFunc) Generate(ctx context.Context, opts GenerateOptions) (*GenerateResult, error) {
	return f(ctx, opts)
}

func (d *mapDispatcher) Dispatch(ctx context.Context, name string, input json.RawMessage) (json.RawMessage, error) {
	d.mu.Lock()
	d.calls = append(d.calls, name)
	d.mu.Unlock()
	if d.handlers == nil || d.handlers[name] == nil {
		return nil, fmt.Errorf("unknown tool %q", name)
	}
	return d.handlers[name](ctx, input)
}

func (d *mapDispatcher) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.calls)
}

func TestREQLOOP001_ConcurrentDispatchPreservesResponseOrder(t *testing.T) {
	var entered int32
	release := make(chan struct{})
	dispatcher := &mapDispatcher{handlers: map[string]func(context.Context, json.RawMessage) (json.RawMessage, error){}}
	for _, name := range []string{"first", "second"} {
		name := name
		dispatcher.handlers[name] = func(context.Context, json.RawMessage) (json.RawMessage, error) {
			if atomic.AddInt32(&entered, 1) == 2 {
				close(release)
			}
			<-release
			return json.RawMessage(`"` + name + `"`), nil
		}
	}
	client := &mockClient{results: []*GenerateResult{
		{FinishReason: FinishReasonToolCalls, Content: []Content{
			ToolUseContent{ID: "1", Name: "first"},
			ToolUseContent{ID: "2", Name: "second"},
		}},
		{FinishReason: FinishReasonStop, Content: []Content{TextContent{Text: "done"}}},
	}}

	messages, _, err := AgentLoop(context.Background(), client, GenerateOptions{}, dispatcher, "", 3)
	if err != nil {
		t.Fatalf("AgentLoop error = %v", err)
	}
	if atomic.LoadInt32(&entered) != 2 {
		t.Fatalf("entered = %d, want 2", entered)
	}
	results := messages[1].Content
	want := []Content{
		ToolResultContent{ToolUseID: "1", Text: `"first"`},
		ToolResultContent{ToolUseID: "2", Text: `"second"`},
	}
	if !reflect.DeepEqual(results, want) {
		t.Fatalf("tool results = %#v, want %#v", results, want)
	}
}

func TestREQLOOP002_ToolErrorBecomesIsErrorResultContinues(t *testing.T) {
	dispatcher := &mapDispatcher{handlers: map[string]func(context.Context, json.RawMessage) (json.RawMessage, error){
		"fail": func(context.Context, json.RawMessage) (json.RawMessage, error) { return nil, errors.New("boom") },
	}}
	client := &mockClient{results: []*GenerateResult{
		{FinishReason: FinishReasonToolCalls, Content: []Content{ToolUseContent{ID: "1", Name: "fail"}}},
		{FinishReason: FinishReasonStop, Content: []Content{TextContent{Text: "recovered"}}},
	}}

	messages, _, err := AgentLoop(context.Background(), client, GenerateOptions{}, dispatcher, "", 3)
	if err != nil {
		t.Fatalf("AgentLoop error = %v", err)
	}
	result := messages[1].Content[0].(ToolResultContent)
	if !result.IsError || result.Text != "boom" {
		t.Fatalf("tool result = %#v, want IsError boom", result)
	}
	if client.callCount() != 2 {
		t.Fatalf("client calls = %d, want 2", client.callCount())
	}
}

func TestREQLOOP002_ToolPanicBecomesIsErrorResultContinues(t *testing.T) {
	dispatcher := &mapDispatcher{handlers: map[string]func(context.Context, json.RawMessage) (json.RawMessage, error){
		"panic": func(context.Context, json.RawMessage) (json.RawMessage, error) { panic("boom") },
	}}
	client := &mockClient{results: []*GenerateResult{
		{FinishReason: FinishReasonToolCalls, Content: []Content{ToolUseContent{ID: "1", Name: "panic"}}},
		{FinishReason: FinishReasonStop, Content: []Content{TextContent{Text: "recovered"}}},
	}}

	messages, _, err := AgentLoop(context.Background(), client, GenerateOptions{}, dispatcher, "", 3)
	if err != nil {
		t.Fatalf("AgentLoop error = %v", err)
	}
	result := messages[1].Content[0].(ToolResultContent)
	if !result.IsError || result.Text != "boom" {
		t.Fatalf("tool result = %#v, want IsError boom", result)
	}
}

func TestREQLOOP003_SubmitResultNotDispatched(t *testing.T) {
	dispatcher := &mapDispatcher{handlers: map[string]func(context.Context, json.RawMessage) (json.RawMessage, error){
		"regular": func(context.Context, json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{}`), nil },
	}}
	submit := json.RawMessage(`{"ok":true}`)
	client := &mockClient{results: []*GenerateResult{{FinishReason: FinishReasonToolCalls, Content: []Content{
		ToolUseContent{ID: "1", Name: "regular"},
		ToolUseContent{ID: "2", Name: "submit_result", Input: submit},
	}}}}

	_, got, err := AgentLoop(context.Background(), client, GenerateOptions{}, dispatcher, "submit_result", 1)
	if err != nil {
		t.Fatalf("AgentLoop error = %v", err)
	}
	if string(got) != string(submit) {
		t.Fatalf("submit = %s, want %s", got, submit)
	}
	if dispatcher.callCount() != 0 {
		t.Fatalf("dispatch calls = %d, want 0", dispatcher.callCount())
	}
}

func TestREQLOOP004_MaxTurnsZeroDisablesLimit(t *testing.T) {
	client := &mockClient{results: []*GenerateResult{
		{FinishReason: FinishReasonToolCalls, Content: []Content{}},
		{FinishReason: FinishReasonStop, Content: []Content{TextContent{Text: "done"}}},
	}}
	_, _, err := AgentLoop(context.Background(), client, GenerateOptions{}, &mapDispatcher{}, "", 0)
	if err != nil {
		t.Fatalf("AgentLoop error = %v", err)
	}
}

func TestREQLOOP005_NoRunStateSideEffects(t *testing.T) {
	input := []Message{{Role: "user", Content: []Content{TextContent{Text: "hello"}}}}
	client := &mockClient{results: []*GenerateResult{{FinishReason: FinishReasonStop}}}
	_, _, err := AgentLoop(context.Background(), client, GenerateOptions{Messages: input}, &mapDispatcher{}, "", 1)
	if err != nil {
		t.Fatalf("AgentLoop error = %v", err)
	}
	if len(input) != 1 || len(input[0].Content) != 1 {
		t.Fatalf("caller messages mutated: %#v", input)
	}
}

func TestREQLOOP006_FinalMessagesFullHistory(t *testing.T) {
	initial := Message{Role: "user", Content: []Content{TextContent{Text: "start"}}}
	client := &mockClient{results: []*GenerateResult{
		{FinishReason: FinishReasonToolCalls, Content: []Content{ToolUseContent{ID: "1", Name: "ok"}}},
		{FinishReason: FinishReasonStop, Content: []Content{TextContent{Text: "done"}}},
	}}
	dispatcher := &mapDispatcher{handlers: map[string]func(context.Context, json.RawMessage) (json.RawMessage, error){
		"ok": func(context.Context, json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`ok`), nil },
	}}
	messages, _, err := AgentLoop(context.Background(), client, GenerateOptions{Messages: []Message{initial}}, dispatcher, "", 3)
	if err != nil {
		t.Fatalf("AgentLoop error = %v", err)
	}
	if len(messages) != 4 {
		t.Fatalf("messages len = %d, want 4: %#v", len(messages), messages)
	}
}

func TestREQLOOP_StopWithRequiredSubmitReturnsErrNoSubmitResult(t *testing.T) {
	client := &mockClient{results: []*GenerateResult{{FinishReason: FinishReasonStop}}}
	_, _, err := AgentLoop(context.Background(), client, GenerateOptions{}, &mapDispatcher{}, "submit_final", 1)
	if !errors.Is(err, ErrNoSubmitResult) || err == nil || !contains(err.Error(), "submit_final") {
		t.Fatalf("error = %v, want ErrNoSubmitResult mentioning submit_final", err)
	}
}

func TestREQLOOP_MaxTurnsExceeded(t *testing.T) {
	client := &mockClient{results: []*GenerateResult{{FinishReason: FinishReasonToolCalls}}}
	_, _, err := AgentLoop(context.Background(), client, GenerateOptions{}, &mapDispatcher{}, "", 1)
	if !errors.Is(err, ErrMaxTurnsExceeded) {
		t.Fatalf("error = %v, want ErrMaxTurnsExceeded", err)
	}
}

func TestREQLOOP_ContextCancelMidTurn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	want := errors.New("model cancelled")
	client := loopClientFunc(func(context.Context, GenerateOptions) (*GenerateResult, error) {
		cancel()
		return nil, want
	})
	_, _, err := AgentLoop(ctx, client, GenerateOptions{}, &mapDispatcher{}, "", 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestREQLOOP_NonSuccessFinishReasonsReturnMessagesAndError(t *testing.T) {
	for _, reason := range []FinishReason{FinishReasonLength, FinishReasonError, FinishReason("other")} {
		t.Run(string(reason), func(t *testing.T) {
			client := &mockClient{results: []*GenerateResult{{FinishReason: reason, Content: []Content{TextContent{Text: "partial"}}}}}
			messages, _, err := AgentLoop(context.Background(), client, GenerateOptions{}, &mapDispatcher{}, "", 1)
			if err == nil {
				t.Fatal("AgentLoop error = nil, want non-nil")
			}
			if len(messages) != 1 {
				t.Fatalf("messages len = %d, want 1", len(messages))
			}
		})
	}
}

func TestREQBUDGET001_EnforcesBeforeEveryGenerate(t *testing.T) {
	var calls int
	counter := func(messages []Message) int {
		calls++
		return 0
	}
	client := &mockClient{results: []*GenerateResult{
		{FinishReason: FinishReasonToolCalls, Content: []Content{}},
		{FinishReason: FinishReasonStop},
	}}
	_, _, err := AgentLoopWithOptions(context.Background(), client, GenerateOptions{}, &mapDispatcher{}, AgentLoopOptions{MaxTurns: 3, TokenBudget: 1, TokenCounter: counter})
	if err != nil {
		t.Fatalf("AgentLoop error = %v", err)
	}
	if calls != 2 {
		t.Fatalf("counter calls = %d, want 2", calls)
	}
}

func TestREQBUDGET002_NeverDropsFirstUser(t *testing.T) {
	first := Message{Role: "user", Content: []Content{TextContent{Text: "must stay"}}}
	client := &mockClient{results: []*GenerateResult{{FinishReason: FinishReasonStop}}}
	_, _, err := AgentLoopWithOptions(context.Background(), client, GenerateOptions{Messages: []Message{first}}, &mapDispatcher{}, AgentLoopOptions{MaxTurns: 1, TokenBudget: 1, TokenCounter: func([]Message) int { return 100 }})
	if err != nil {
		t.Fatalf("AgentLoop error = %v", err)
	}
	got := client.optionAt(0).Messages
	if len(got) != 1 || got[0].Role != "user" {
		t.Fatalf("messages = %#v, want first user retained", got)
	}
}

func TestREQBUDGET003_OldestPairFirstProceedsIfStillOver(t *testing.T) {
	messages := []Message{
		{Role: "user", Content: []Content{TextContent{Text: "first"}}},
		{Role: "assistant", Content: []Content{ToolUseContent{ID: "old", Name: "tool"}}},
		{Role: "user", Content: []Content{ToolResultContent{ToolUseID: "old", Text: "old result"}}},
		{Role: "assistant", Content: []Content{ToolUseContent{ID: "new", Name: "tool"}}},
		{Role: "user", Content: []Content{ToolResultContent{ToolUseID: "new", Text: "new result"}}},
	}
	client := &mockClient{results: []*GenerateResult{{FinishReason: FinishReasonStop}}}
	_, _, err := AgentLoopWithOptions(context.Background(), client, GenerateOptions{Messages: messages}, &mapDispatcher{}, AgentLoopOptions{MaxTurns: 1, TokenBudget: 1, TokenCounter: func([]Message) int { return 100 }})
	if err != nil {
		t.Fatalf("AgentLoop error = %v", err)
	}
	got := client.optionAt(0).Messages
	if len(got) != 1 || got[0].Role != "user" {
		t.Fatalf("messages after trimming = %#v, want only first user", got)
	}
}

func TestREQBUDGET004_NilCounterUsesCharHeuristic(t *testing.T) {
	messages := []Message{
		{Role: "user", Content: []Content{TextContent{Text: "first"}}},
		{Role: "assistant", Content: []Content{ToolUseContent{ID: "old", Name: "tool", Input: json.RawMessage(`{"long":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}`)}}},
		{Role: "user", Content: []Content{ToolResultContent{ToolUseID: "old", Text: "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}}},
	}
	client := &mockClient{results: []*GenerateResult{{FinishReason: FinishReasonStop}}}
	_, _, err := AgentLoopWithOptions(context.Background(), client, GenerateOptions{Messages: messages}, &mapDispatcher{}, AgentLoopOptions{MaxTurns: 1, TokenBudget: 2})
	if err != nil {
		t.Fatalf("AgentLoop error = %v", err)
	}
	if len(client.optionAt(0).Messages) != 1 {
		t.Fatalf("messages = %#v, want heuristic to trim tool pair", client.optionAt(0).Messages)
	}
}

func TestREQBUDGET005_ZeroBudgetNoOp(t *testing.T) {
	called := false
	client := &mockClient{results: []*GenerateResult{{FinishReason: FinishReasonStop}}}
	_, _, err := AgentLoopWithOptions(context.Background(), client, GenerateOptions{Messages: []Message{{Role: "user"}}}, &mapDispatcher{}, AgentLoopOptions{MaxTurns: 1, TokenCounter: func([]Message) int { called = true; return 100 }})
	if err != nil {
		t.Fatalf("AgentLoop error = %v", err)
	}
	if called {
		t.Fatal("TokenCounter called with zero budget")
	}
}

func contains(s, substr string) bool {
	return len(substr) == 0 || len(s) >= len(substr) && (s == substr || contains(s[1:], substr) || s[:len(substr)] == substr)
}
