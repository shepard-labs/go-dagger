// Package llm defines provider-neutral LLM requests, tool calls, and agent loops.
package llm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// Client is the single interface for LLM completion calls.
type Client interface {
	Generate(ctx context.Context, opts GenerateOptions) (*GenerateResult, error)
}

// GenerateOptions contains the prompt, tools, and limits for one generation call.
type GenerateOptions struct {
	System    string
	Messages  []Message
	Tools     []Tool
	MaxTokens int
}

// Message is a role-tagged item in the LLM conversation history.
type Message struct {
	Role    string
	Content []Content
}

// Content is one typed part of a message.
type Content interface{ isContent() }

// TextContent is plain text in a user or assistant message.
type TextContent struct {
	Text string
}

// ToolUseContent is an assistant request to invoke a tool.
type ToolUseContent struct {
	ID    string
	Name  string
	Input json.RawMessage
}

// ToolResultContent is the user-side response to a tool invocation.
type ToolResultContent struct {
	ToolUseID string
	Text      string
	IsError   bool
}

func (TextContent) isContent()       {}
func (ToolUseContent) isContent()    {}
func (ToolResultContent) isContent() {}

// Tool describes an LLM-callable tool and its JSON input schema.
type Tool struct {
	Name        string
	Description string
	InputSchema json.RawMessage
}

// GenerateResult is the normalized response returned by a Client.
type GenerateResult struct {
	Content      []Content
	FinishReason FinishReason
	Usage        Usage
}

// FinishReason explains why a generation call stopped.
type FinishReason string

const (
	// FinishReasonStop means the model completed normally.
	FinishReasonStop FinishReason = "stop"
	// FinishReasonToolCalls means the model emitted one or more tool calls.
	FinishReasonToolCalls FinishReason = "tool-calls"
	// FinishReasonLength means generation stopped at a token limit.
	FinishReasonLength FinishReason = "length"
	// FinishReasonError means the provider reported an error finish state.
	FinishReasonError FinishReason = "error"
)

// Usage records token counts reported by the provider.
type Usage struct {
	InputTokens  int
	OutputTokens int
}

// FailoverConfig controls when and how WithFailover switches clients.
type FailoverConfig struct {
	ShouldFailover func(ctx context.Context, err error) bool
	GetNext        func(attempt int) Client
	MaxAttempts    int
}

// WithFailover wraps a client with retry-on-provider-failure behavior.
func WithFailover(client Client, cfg FailoverConfig) Client {
	return failoverClient{client: client, cfg: cfg}
}

type failoverClient struct {
	client Client
	cfg    FailoverConfig
}

func (c failoverClient) Generate(ctx context.Context, opts GenerateOptions) (*GenerateResult, error) {
	client := c.client
	attempt := 0
	var lastErr error
	for client != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result, err := client.Generate(ctx, opts)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if c.cfg.ShouldFailover == nil || !c.cfg.ShouldFailover(ctx, err) {
			return nil, err
		}
		attempt++
		if c.cfg.MaxAttempts > 0 && attempt > c.cfg.MaxAttempts {
			return nil, lastErr
		}
		if c.cfg.GetNext == nil {
			return nil, lastErr
		}
		client = c.cfg.GetNext(attempt)
	}
	return nil, lastErr
}

// CacheBackend stores GenerateResult values by deterministic request key.
type CacheBackend interface {
	Get(ctx context.Context, key string) (*GenerateResult, bool)
	Set(ctx context.Context, key string, result *GenerateResult)
}

// WithCache wraps a client with read-through response caching.
func WithCache(client Client, backend CacheBackend) Client {
	if backend == nil {
		panic("llm: nil cache backend")
	}
	return cacheClient{client: client, backend: backend}
}

type cacheClient struct {
	client  Client
	backend CacheBackend
}

func (c cacheClient) Generate(ctx context.Context, opts GenerateOptions) (*GenerateResult, error) {
	key, err := cacheKey(opts)
	if err != nil {
		return nil, err
	}
	if result, ok := c.backend.Get(ctx, key); ok {
		return result, nil
	}
	result, err := c.client.Generate(ctx, opts)
	if err != nil {
		return nil, err
	}
	if result != nil {
		c.backend.Set(ctx, key, result)
	}
	return result, nil
}

func cacheKey(opts GenerateOptions) (string, error) {
	data, err := json.Marshal(opts)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
