package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

type ToolDispatcher interface {
	Dispatch(ctx context.Context, name string, input json.RawMessage) (json.RawMessage, error)
}

var (
	ErrMaxTurnsExceeded = errors.New("llm: agent loop exceeded max turns")
	ErrNoSubmitResult   = errors.New("llm: loop ended without calling submit result tool")
)

type AgentLoopOptions struct {
	SubmitResultTool string
	MaxTurns         int
	TokenBudget      int
	TokenCounter     func(messages []Message) int
}

func AgentLoop(ctx context.Context, client Client, opts GenerateOptions, dispatcher ToolDispatcher, submitResultTool string, maxTurns int) ([]Message, json.RawMessage, error) {
	return AgentLoopWithOptions(ctx, client, opts, dispatcher, AgentLoopOptions{SubmitResultTool: submitResultTool, MaxTurns: maxTurns})
}

func AgentLoopWithOptions(ctx context.Context, client Client, opts GenerateOptions, dispatcher ToolDispatcher, loopOpts AgentLoopOptions) ([]Message, json.RawMessage, error) {
	messages := cloneMessages(opts.Messages)
	turns := 0
	for {
		if loopOpts.MaxTurns > 0 && turns >= loopOpts.MaxTurns {
			return messages, nil, ErrMaxTurnsExceeded
		}
		if err := ctx.Err(); err != nil {
			return messages, nil, err
		}
		messages = enforceTokenBudget(messages, loopOpts.TokenBudget, loopOpts.TokenCounter)
		request := opts
		request.Messages = cloneMessages(messages)
		result, err := client.Generate(ctx, request)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return messages, nil, ctxErr
			}
			return messages, nil, err
		}
		turns++
		assistant := Message{Role: "assistant", Content: cloneContent(result.Content)}
		messages = append(messages, assistant)

		if loopOpts.SubmitResultTool != "" {
			if input, ok := findSubmitResult(result.Content, loopOpts.SubmitResultTool); ok {
				return messages, input, nil
			}
		}

		switch result.FinishReason {
		case FinishReasonToolCalls:
			toolCalls := toolUseContents(result.Content)
			if len(toolCalls) == 0 {
				continue
			}
			messages = append(messages, Message{Role: "user", Content: dispatchToolCalls(ctx, dispatcher, toolCalls)})
		case FinishReasonStop:
			if loopOpts.SubmitResultTool != "" {
				return messages, nil, fmt.Errorf("%w: %s", ErrNoSubmitResult, loopOpts.SubmitResultTool)
			}
			return messages, nil, nil
		case FinishReasonLength:
			return messages, nil, fmt.Errorf("llm: finish reason %s", FinishReasonLength)
		case FinishReasonError:
			return messages, nil, fmt.Errorf("llm: finish reason %s", FinishReasonError)
		default:
			return messages, nil, fmt.Errorf("llm: finish reason %s", result.FinishReason)
		}
	}
}

func findSubmitResult(contents []Content, name string) (json.RawMessage, bool) {
	for _, content := range contents {
		toolUse, ok := content.(ToolUseContent)
		if ok && toolUse.Name == name {
			return cloneRawMessage(toolUse.Input), true
		}
	}
	return nil, false
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
