package llm

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/shepard-labs/go-ai-sdk/anthropic"
)

// AnthropicAdapter adapts go-ai-sdk Anthropic models to the Client interface.
type AnthropicAdapter struct {
	model anthropic.LanguageModel
}

// AnthropicModelID names an Anthropic model accepted by NewAnthropicClient.
type AnthropicModelID = anthropic.ModelID

const (
	// AnthropicModelClaudeHaiku45 is the Claude Haiku model identifier.
	AnthropicModelClaudeHaiku45 = anthropic.ModelClaudeHaiku45
	// AnthropicModelClaudeSonnet46 is the Claude Sonnet model identifier.
	AnthropicModelClaudeSonnet46 = anthropic.ModelClaudeSonnet46
	// AnthropicModelClaudeOpus48 is the Claude Opus model identifier.
	AnthropicModelClaudeOpus48 = anthropic.ModelClaudeOpus48
)

// NewAnthropicAdapter wraps an existing Anthropic language model as a Client.
func NewAnthropicAdapter(model anthropic.LanguageModel) Client {
	return &AnthropicAdapter{model: model}
}

// NewAnthropicClient creates an Anthropic-backed Client from an API key and model ID.
func NewAnthropicClient(apiKey string, modelID AnthropicModelID) (Client, error) {
	provider := anthropic.CreateAnthropic(anthropic.ProviderSettings{APIKey: apiKey})
	if err := provider.Err(); err != nil {
		return nil, err
	}
	return NewAnthropicAdapter(provider.Model(string(modelID))), nil
}

// Generate sends a completion request through the Anthropic SDK.
func (a *AnthropicAdapter) Generate(ctx context.Context, opts GenerateOptions) (*GenerateResult, error) {
	sdkOpts, err := toAnthropicOptions(opts)
	if err != nil {
		return nil, err
	}
	result, err := a.model.DoGenerate(ctx, sdkOpts)
	if err != nil {
		return nil, err
	}
	return fromAnthropicResult(result), nil
}

// GeneratorFunc adapts a function to the Client interface.
type GeneratorFunc func(ctx context.Context, opts GenerateOptions) (*GenerateResult, error)

// Generate calls f with the provided options.
func (f GeneratorFunc) Generate(ctx context.Context, opts GenerateOptions) (*GenerateResult, error) {
	return f(ctx, opts)
}

func toAnthropicOptions(opts GenerateOptions) (anthropic.GenerateOptions, error) {
	messages := make([]anthropic.Message, 0, len(opts.Messages)+1)
	if opts.System != "" {
		messages = append(messages, anthropic.SystemMessage{Content: opts.System})
	}
	for _, message := range opts.Messages {
		sdkMessage, err := toAnthropicMessage(message)
		if err != nil {
			return anthropic.GenerateOptions{}, err
		}
		messages = append(messages, sdkMessage)
	}
	tools, err := toAnthropicTools(opts.Tools)
	if err != nil {
		return anthropic.GenerateOptions{}, err
	}
	return anthropic.GenerateOptions{Messages: messages, Tools: tools, MaxTokens: opts.MaxTokens}, nil
}

func toAnthropicMessage(message Message) (anthropic.Message, error) {
	switch message.Role {
	case "user":
		contents := make([]anthropic.UserContent, 0, len(message.Content))
		for _, content := range message.Content {
			sdkContent, err := toAnthropicUserContent(content)
			if err != nil {
				return nil, err
			}
			contents = append(contents, sdkContent)
		}
		return anthropic.UserMessage{Content: contents}, nil
	case "assistant":
		contents := make([]anthropic.AssistantContent, 0, len(message.Content))
		for _, content := range message.Content {
			sdkContent, err := toAnthropicAssistantContent(content)
			if err != nil {
				return nil, err
			}
			contents = append(contents, sdkContent)
		}
		return anthropic.AssistantMessage{Content: contents}, nil
	default:
		return nil, fmt.Errorf("llm: unknown message role %q", message.Role)
	}
}

func toAnthropicUserContent(content Content) (anthropic.UserContent, error) {
	switch c := content.(type) {
	case TextContent:
		return anthropic.TextContent{Text: c.Text}, nil
	case ToolResultContent:
		return anthropic.ToolResultContent{ToolCallID: c.ToolUseID, Result: []anthropic.ToolResultPart{anthropic.ToolResultText{Text: c.Text}}, IsError: c.IsError}, nil
	default:
		return nil, fmt.Errorf("llm: unsupported user content %T", content)
	}
}

func toAnthropicAssistantContent(content Content) (anthropic.AssistantContent, error) {
	switch c := content.(type) {
	case TextContent:
		return anthropic.TextContent{Text: c.Text}, nil
	case ToolUseContent:
		return anthropic.ToolCallContent{ToolCallID: c.ID, ToolName: c.Name, Input: cloneRawMessage(c.Input)}, nil
	default:
		return nil, fmt.Errorf("llm: unsupported assistant content %T", content)
	}
}

func toAnthropicTools(tools []Tool) ([]anthropic.Tool, error) {
	sdkTools := make([]anthropic.Tool, len(tools))
	for i, tool := range tools {
		schema, err := decodeSchema(tool.InputSchema)
		if err != nil {
			return nil, err
		}
		sdkTools[i] = anthropic.Tool{Name: tool.Name, Description: tool.Description, InputSchema: schema}
	}
	return sdkTools, nil
}

func decodeSchema(schema json.RawMessage) (any, error) {
	if len(schema) == 0 {
		return nil, nil
	}
	var decoded any
	if err := json.Unmarshal(schema, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func fromAnthropicResult(result *anthropic.GenerateResult) *GenerateResult {
	if result == nil {
		return nil
	}
	return &GenerateResult{Content: fromAnthropicContent(result.Content), FinishReason: fromAnthropicFinishReason(result.FinishReason), Usage: Usage{InputTokens: result.Usage.InputTokens.Total, OutputTokens: result.Usage.OutputTokens.Total}}
}

func fromAnthropicContent(contents []anthropic.Content) []Content {
	converted := make([]Content, 0, len(contents))
	for _, content := range contents {
		switch c := content.(type) {
		case anthropic.TextContent:
			converted = append(converted, TextContent{Text: c.Text})
		case anthropic.ToolCallContent:
			converted = append(converted, ToolUseContent{ID: c.ToolCallID, Name: c.ToolName, Input: cloneRawMessage(c.Input)})
		case anthropic.ToolResultContent:
			converted = append(converted, ToolResultContent{ToolUseID: c.ToolCallID, Text: toolResultText(c.Result), IsError: c.IsError})
		}
	}
	return converted
}

func toolResultText(parts []anthropic.ToolResultPart) string {
	for _, part := range parts {
		if text, ok := part.(anthropic.ToolResultText); ok {
			return text.Text
		}
	}
	return ""
}

func fromAnthropicFinishReason(reason anthropic.FinishReason) FinishReason {
	switch reason {
	case anthropic.FinishReasonStop:
		return FinishReasonStop
	case anthropic.FinishReasonToolCalls:
		return FinishReasonToolCalls
	case anthropic.FinishReasonLength:
		return FinishReasonLength
	case anthropic.FinishReasonError:
		return FinishReasonError
	default:
		return FinishReasonError
	}
}
