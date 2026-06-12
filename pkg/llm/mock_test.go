package llm

import (
	"context"
	"sync"
)

type mockClient struct {
	mu      sync.Mutex
	calls   int
	options []GenerateOptions
	results []*GenerateResult
	errors  []error
}

func (m *mockClient) Generate(ctx context.Context, opts GenerateOptions) (*GenerateResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	m.options = append(m.options, opts)
	idx := m.calls - 1
	var result *GenerateResult
	if idx < len(m.results) {
		result = m.results[idx]
	}
	var err error
	if idx < len(m.errors) {
		err = m.errors[idx]
	}
	return result, err
}

func (m *mockClient) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func (m *mockClient) optionAt(i int) GenerateOptions {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.options[i]
}

type memoryCache struct {
	mu   sync.Mutex
	data map[string]*GenerateResult
}

func newMemoryCache() *memoryCache {
	return &memoryCache{data: map[string]*GenerateResult{}}
}

func (m *memoryCache) Get(ctx context.Context, key string) (*GenerateResult, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result, ok := m.data[key]
	return result, ok
}

func (m *memoryCache) Set(ctx context.Context, key string, result *GenerateResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = result
}

func (m *memoryCache) len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.data)
}
