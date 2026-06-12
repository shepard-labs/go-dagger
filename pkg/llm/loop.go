package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// ToolDispatcher executes model-requested tool calls.
type ToolDispatcher interface {
	Dispatch(ctx context.Context, name string, input json.RawMessage) (json.RawMessage, error)
}

var (
	// ErrMaxTurnsExceeded indicates the agent loop hit its turn limit.
	ErrMaxTurnsExceeded = errors.New("llm: agent loop exceeded max turns")
	// ErrNoSubmitResult indicates the model stopped before calling a terminal tool.
	ErrNoSubmitResult = errors.New("llm: loop ended without calling submit result tool")
	// ErrMaxToolRepairsExceeded indicates validation repair attempts were exhausted.
	ErrMaxToolRepairsExceeded = errors.New("llm: agent loop exceeded max tool repairs")
)

// ToolPolicy controls validation and terminal behavior for one tool.
type ToolPolicy struct {
	Terminal bool
	Validate func(input json.RawMessage) error
}

// AgentLoopResult records the final transcript and terminal tool call details.
type AgentLoopResult struct {
	Messages  []Message
	ToolName  string
	ToolUseID string
	Input     json.RawMessage
	Turns     int
	Repairs   int
}

// AgentLoopOptions configures multi-turn tool use and terminal submission behavior.
type AgentLoopOptions struct {
	SubmitResultTool string
	MaxTurns         int
	TokenBudget      int
	TokenCounter     func(messages []Message) int
	ToolPolicies     map[string]ToolPolicy
	MaxToolRepairs   int
}

// AgentLoop runs a tool-calling loop until the submit-result tool is called.
func AgentLoop(ctx context.Context, client Client, opts GenerateOptions, dispatcher ToolDispatcher, submitResultTool string, maxTurns int) ([]Message, json.RawMessage, error) {
	return AgentLoopWithOptions(ctx, client, opts, dispatcher, AgentLoopOptions{SubmitResultTool: submitResultTool, MaxTurns: maxTurns})
}

// AgentLoopWithOptions runs AgentLoop with the expanded options struct.
func AgentLoopWithOptions(ctx context.Context, client Client, opts GenerateOptions, dispatcher ToolDispatcher, loopOpts AgentLoopOptions) ([]Message, json.RawMessage, error) {
	result, err := AgentLoopResultWithOptions(ctx, client, opts, dispatcher, loopOpts)
	return result.Messages, result.Input, err
}

// AgentLoopResultWithOptions returns transcript metadata in addition to terminal input.
func AgentLoopResultWithOptions(ctx context.Context, client Client, opts GenerateOptions, dispatcher ToolDispatcher, loopOpts AgentLoopOptions) (AgentLoopResult, error) {
	messages := cloneMessages(opts.Messages)
	turns := 0
	repairs := 0
	policies := normalizeToolPolicies(loopOpts)
	terminalTools := terminalToolNames(policies)
	for {
		if loopOpts.MaxTurns > 0 && turns >= loopOpts.MaxTurns {
			return AgentLoopResult{Messages: messages, Turns: turns, Repairs: repairs}, ErrMaxTurnsExceeded
		}
		if err := ctx.Err(); err != nil {
			return AgentLoopResult{Messages: messages, Turns: turns, Repairs: repairs}, err
		}
		messages = enforceTokenBudget(messages, loopOpts.TokenBudget, loopOpts.TokenCounter)
		request := opts
		request.Messages = cloneMessages(messages)
		result, err := client.Generate(ctx, request)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return AgentLoopResult{Messages: messages, Turns: turns, Repairs: repairs}, ctxErr
			}
			return AgentLoopResult{Messages: messages, Turns: turns, Repairs: repairs}, err
		}
		turns++
		assistant := Message{Role: "assistant", Content: cloneContent(result.Content)}
		messages = append(messages, assistant)

		toolCalls := toolUseContents(result.Content)
		if len(toolCalls) > 0 && (result.FinishReason == FinishReasonToolCalls || hasTerminalToolCall(toolCalls, policies)) {
			processed, err := processToolCalls(ctx, dispatcher, toolCalls, policies, loopOpts, &repairs)
			if err != nil {
				return AgentLoopResult{Messages: messages, Turns: turns, Repairs: repairs}, err
			}
			if processed.terminal != nil {
				terminal := *processed.terminal
				return AgentLoopResult{Messages: messages, ToolName: terminal.Name, ToolUseID: terminal.ID, Input: cloneRawMessage(terminal.Input), Turns: turns, Repairs: repairs}, nil
			}
			if len(processed.results) > 0 {
				messages = append(messages, Message{Role: "user", Content: processed.results})
				continue
			}
		}

		switch result.FinishReason {
		case FinishReasonToolCalls:
			continue
		case FinishReasonStop:
			if len(terminalTools) > 0 {
				return AgentLoopResult{Messages: messages, Turns: turns, Repairs: repairs}, fmt.Errorf("%w: %s", ErrNoSubmitResult, strings.Join(terminalTools, ", "))
			}
			return AgentLoopResult{Messages: messages, Turns: turns, Repairs: repairs}, nil
		case FinishReasonLength:
			return AgentLoopResult{Messages: messages, Turns: turns, Repairs: repairs}, fmt.Errorf("llm: finish reason %s", FinishReasonLength)
		case FinishReasonError:
			return AgentLoopResult{Messages: messages, Turns: turns, Repairs: repairs}, fmt.Errorf("llm: finish reason %s", FinishReasonError)
		default:
			return AgentLoopResult{Messages: messages, Turns: turns, Repairs: repairs}, fmt.Errorf("llm: finish reason %s", result.FinishReason)
		}
	}
}

type processedToolCalls struct {
	terminal *ToolUseContent
	results  []Content
}

func normalizeToolPolicies(opts AgentLoopOptions) map[string]ToolPolicy {
	policies := make(map[string]ToolPolicy, len(opts.ToolPolicies)+1)
	for name, policy := range opts.ToolPolicies {
		policies[name] = policy
	}
	if opts.SubmitResultTool != "" {
		policy := policies[opts.SubmitResultTool]
		policy.Terminal = true
		policies[opts.SubmitResultTool] = policy
	}
	return policies
}

func terminalToolNames(policies map[string]ToolPolicy) []string {
	names := make([]string, 0)
	for name, policy := range policies {
		if policy.Terminal {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func hasTerminalToolCall(toolCalls []ToolUseContent, policies map[string]ToolPolicy) bool {
	for _, toolCall := range toolCalls {
		if policies[toolCall.Name].Terminal {
			return true
		}
	}
	return false
}

func processToolCalls(ctx context.Context, dispatcher ToolDispatcher, toolCalls []ToolUseContent, policies map[string]ToolPolicy, opts AgentLoopOptions, repairs *int) (processedToolCalls, error) {
	results := make([]Content, len(toolCalls))
	validDispatchCalls := make([]indexedToolCall, 0, len(toolCalls))
	for i, toolCall := range toolCalls {
		policy := policies[toolCall.Name]
		if policy.Validate != nil {
			if err := policy.Validate(cloneRawMessage(toolCall.Input)); err != nil {
				(*repairs)++
				if opts.MaxToolRepairs > 0 && *repairs > opts.MaxToolRepairs {
					return processedToolCalls{}, fmt.Errorf("%w: %s: %v", ErrMaxToolRepairsExceeded, toolCall.Name, err)
				}
				results[i] = ToolResultContent{ToolUseID: toolCall.ID, Text: fmt.Sprintf("invalid tool input for %s: %v", toolCall.Name, err), IsError: true}
				continue
			}
		}
		if policy.Terminal {
			terminal := toolCall
			terminal.Input = cloneRawMessage(toolCall.Input)
			return processedToolCalls{terminal: &terminal}, nil
		}
		validDispatchCalls = append(validDispatchCalls, indexedToolCall{index: i, toolCall: toolCall})
	}
	if len(validDispatchCalls) > 0 {
		dispatchResults := dispatchIndexedToolCalls(ctx, dispatcher, validDispatchCalls)
		for i, result := range dispatchResults {
			results[validDispatchCalls[i].index] = result
		}
	}
	return processedToolCalls{results: compactContent(results)}, nil
}

type indexedToolCall struct {
	index    int
	toolCall ToolUseContent
}

func dispatchIndexedToolCalls(ctx context.Context, dispatcher ToolDispatcher, toolCalls []indexedToolCall) []Content {
	results := make([]Content, len(toolCalls))
	var wg sync.WaitGroup
	for i, toolCall := range toolCalls {
		wg.Add(1)
		go func(i int, toolCall ToolUseContent) {
			defer wg.Done()
			results[i] = dispatchToolCall(ctx, dispatcher, toolCall)
		}(i, toolCall.toolCall)
	}
	wg.Wait()
	return results
}

func compactContent(contents []Content) []Content {
	compacted := make([]Content, 0, len(contents))
	for _, content := range contents {
		if content != nil {
			compacted = append(compacted, content)
		}
	}
	return compacted
}

func toolUseContents(contents []Content) []ToolUseContent {
	var toolCalls []ToolUseContent
	for _, content := range contents {
		toolUse, ok := content.(ToolUseContent)
		if ok {
			toolCalls = append(toolCalls, toolUse)
		}
	}
	return toolCalls
}

func dispatchToolCalls(ctx context.Context, dispatcher ToolDispatcher, toolCalls []ToolUseContent) []Content {
	results := make([]Content, len(toolCalls))
	var wg sync.WaitGroup
	for i, toolCall := range toolCalls {
		wg.Add(1)
		go func(i int, toolCall ToolUseContent) {
			defer wg.Done()
			results[i] = dispatchToolCall(ctx, dispatcher, toolCall)
		}(i, toolCall)
	}
	wg.Wait()
	return results
}

func dispatchToolCall(ctx context.Context, dispatcher ToolDispatcher, toolCall ToolUseContent) (result Content) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = ToolResultContent{ToolUseID: toolCall.ID, Text: fmt.Sprint(recovered), IsError: true}
		}
	}()
	if dispatcher == nil {
		return ToolResultContent{ToolUseID: toolCall.ID, Text: "tool dispatcher is nil", IsError: true}
	}
	output, err := dispatcher.Dispatch(ctx, toolCall.Name, cloneRawMessage(toolCall.Input))
	if err != nil {
		return ToolResultContent{ToolUseID: toolCall.ID, Text: err.Error(), IsError: true}
	}
	return ToolResultContent{ToolUseID: toolCall.ID, Text: string(output)}
}

func enforceTokenBudget(messages []Message, budget int, counter func([]Message) int) []Message {
	if budget <= 0 {
		return messages
	}
	if counter == nil {
		counter = estimateMessageTokens
	}
	trimmed := cloneMessages(messages)
	for counter(trimmed) > budget {
		idx := oldestToolPairIndex(trimmed)
		if idx < 0 {
			return trimmed
		}
		trimmed = append(trimmed[:idx], trimmed[idx+2:]...)
	}
	return trimmed
}

func oldestToolPairIndex(messages []Message) int {
	for i := 1; i < len(messages)-1; i++ {
		if messages[i].Role != "assistant" || messages[i+1].Role != "user" {
			continue
		}
		if hasToolUse(messages[i].Content) && hasToolResult(messages[i+1].Content) {
			return i
		}
	}
	return -1
}

func hasToolUse(contents []Content) bool {
	for _, content := range contents {
		if _, ok := content.(ToolUseContent); ok {
			return true
		}
	}
	return false
}

func hasToolResult(contents []Content) bool {
	for _, content := range contents {
		if _, ok := content.(ToolResultContent); ok {
			return true
		}
	}
	return false
}

func estimateMessageTokens(messages []Message) int {
	chars := 0
	for _, message := range messages {
		chars += len(message.Role)
		for _, content := range message.Content {
			switch c := content.(type) {
			case TextContent:
				chars += len(c.Text)
			case ToolUseContent:
				chars += len(c.ID) + len(c.Name) + len(c.Input)
			case ToolResultContent:
				chars += len(c.ToolUseID) + len(c.Text)
			}
		}
	}
	return (chars + 3) / 4
}

func cloneMessages(messages []Message) []Message {
	if messages == nil {
		return nil
	}
	cloned := make([]Message, len(messages))
	for i, message := range messages {
		cloned[i] = Message{Role: message.Role, Content: cloneContent(message.Content)}
	}
	return cloned
}

func cloneContent(contents []Content) []Content {
	if contents == nil {
		return nil
	}
	cloned := make([]Content, len(contents))
	for i, content := range contents {
		switch c := content.(type) {
		case TextContent:
			cloned[i] = c
		case ToolUseContent:
			c.Input = cloneRawMessage(c.Input)
			cloned[i] = c
		case ToolResultContent:
			cloned[i] = c
		default:
			cloned[i] = content
		}
	}
	return cloned
}

func cloneRawMessage(input json.RawMessage) json.RawMessage {
	if input == nil {
		return nil
	}
	return append(json.RawMessage(nil), input...)
}
