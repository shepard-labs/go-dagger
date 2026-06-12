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

type GenerateOptions struct {
	System    string
	Messages  []Message
	Tools     []Tool
	MaxTokens int
}

type Message struct {
	Role    string
	Content []Content
}

type Content interface{ isContent() }

type TextContent struct {
	Text string
}

type ToolUseContent struct {
	ID    string
	Name  string
	Input json.RawMessage
}

type ToolResultContent struct {
	ToolUseID string
	Text      string
	IsError   bool
}

func (TextContent) isContent()       {}
func (ToolUseContent) isContent()    {}
func (ToolResultContent) isContent() {}

type Tool struct {
	Name        string
	Description string
	InputSchema json.RawMessage
}

type GenerateResult struct {
	Content      []Content
	FinishReason FinishReason
	Usage        Usage
}

type FinishReason string

const (
	FinishReasonStop      FinishReason = "stop"
	FinishReasonToolCalls FinishReason = "tool-calls"
	FinishReasonLength    FinishReason = "length"
	FinishReasonError     FinishReason = "error"
)

type Usage struct {
	InputTokens  int
	OutputTokens int
}

type FailoverConfig struct {
	ShouldFailover func(ctx context.Context, err error) bool
	GetNext        func(attempt int) Client
	MaxAttempts    int
}

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

type CacheBackend interface {
	Get(ctx context.Context, key string) (*GenerateResult, bool)
	Set(ctx context.Context, key string, result *GenerateResult)
}

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
