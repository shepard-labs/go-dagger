# go-ai-sdk: Schema Generation, Toolkits, Run Persistence, Provider Registry

**Date:** 2026-06-16  
**Status:** Approved

## Background

Four features present in aisuite (Python) are missing from go-ai-sdk:

| Feature | aisuite | go-ai-sdk (current) |
|---|---|---|
| Auto tool schema generation | Reflection from type hints | Manual `json.RawMessage` |
| Prebuilt toolkits | files, git, shell | None |
| Agent run persistence | In-memory, file, Postgres | None |
| Provider auto-discovery | Filename convention + importlib | Explicit instantiation only |

This spec designs Go-idiomatic equivalents for all four.

---

## Module Layout

All new code lives under `llm/` in the existing `go-ai-sdk` module. Backend submodules carry their own `go.mod` to avoid pulling heavy deps (pgx, cloud SDKs) into the core module.

```
go-ai-sdk/
├── llm/                              # existing — no new deps
│   ├── schema/                       # NEW: struct-tag → llm.Tool schema generation (pure reflect)
│   ├── toolkit/                      # NEW: files, shell, git toolkits (no new deps)
│   ├── registry/                     # NEW: provider registry (no new deps)
│   └── store/                        # NEW: RunStore interface + memory + file subpackages (no new deps)
│       ├── store.go                  # RunStore interface, RunState type
│       ├── memory/                   # subpackage — no deps
│       ├── file/                     # subpackage — no deps
│       ├── postgres/                 # submodule — deps: pgx
│       ├── gcs/                      # submodule — deps: google-cloud-storage
│       └── r2/                       # submodule — deps: aws-sdk-go-v2/s3
├── anthropic/adapter/                # NEW: blank-import to register anthropic
├── openai/adapter/                   # NEW: blank-import to register openai
├── google/adapter/                   # NEW: blank-import to register google
├── cohere/adapter/                   # NEW: blank-import to register cohere
├── openrouter/adapter/               # NEW: blank-import to register openrouter
└── openaicompatible/adapter/         # NEW: blank-import to register openaicompatible
```

---

## 1. Auto Tool Schema Generation (`llm/schema`)

### Goal

Replace manual `json.RawMessage` schema authoring with a typed Go struct approach.

### API

```go
tool, err := schema.Tool("search_web", "Search the web", SearchInput{})
```

Where `SearchInput` is a plain Go struct:

```go
type SearchInput struct {
    Query  string `json:"query"  description:"search query"`
    Limit  int    `json:"limit"  description:"max results" minimum:"1" maximum:"100"`
}
```

### Struct Tag Reference

| Tag | JSON Schema field | Notes |
|---|---|---|
| `json:"name"` | property key | required |
| `description:"..."` | `description` | |
| `minimum:"n"` | `minimum` | numeric only |
| `maximum:"n"` | `maximum` | numeric only |
| `enum:"a,b,c"` | `enum` | comma-separated |
| `required:"true"` | forces required | pointer fields are optional by default |

### Type Mapping

| Go type | JSON Schema type |
|---|---|
| `string` | `"string"` |
| `int`, `int64`, `float64` | `"number"` |
| `bool` | `"boolean"` |
| `[]T` | `"array"` with `items` |
| struct | `"object"` with `properties` |
| `*T` | same as `T`, not required |

### Behaviour

- Non-pointer fields are required by default.
- Pointer fields are optional.
- Nested structs generate nested `"object"` schemas recursively.
- Returns `(llm.Tool, error)`; errors on unsupported types or malformed tags.

---

## 2. Prebuilt Toolkits (`llm/toolkit`)

### Goal

Ready-made, scoped tool sets for common agent tasks.

### API

Each toolkit constructor returns a value that implements both a `Tools() []llm.Tool` method and `llm.ToolDispatcher`.

```go
files := toolkit.Files(toolkit.FilesConfig{Roots: []string{"/workspace"}})
shell := toolkit.Shell(toolkit.ShellConfig{Cwd: "/workspace", AllowedCmds: []string{"ls", "cat"}})
git   := toolkit.Git(toolkit.GitConfig{Root: "/workspace"})

dispatcher := toolkit.Merge(files, shell, git)
tools      := toolkit.Tools(files, shell, git) // []llm.Tool

llm.AgentLoopWithOptions(ctx, client, llm.GenerateOptions{
    Tools: tools,
    ...
}, dispatcher, loopOpts)
```

### Files Toolkit

Tools: `read_file`, `write_file`, `list_dir`, `search_files`

Config:
```go
type FilesConfig struct {
    Roots       []string // allowed root paths; path traversal outside these is an error
    MaxReadBytes int     // default 1MB
}
```

### Shell Toolkit

Tools: `run_command`

Config:
```go
type ShellConfig struct {
    Cwd         string
    AllowedCmds []string      // if non-empty, only these base commands are permitted
    Timeout     time.Duration // default 30s
}
```

Returns stdout, stderr, and exit code as structured JSON.

### Git Toolkit

Tools: `git_status`, `git_diff`, `git_log`

Config:
```go
type GitConfig struct {
    Root string // repo root; all operations scoped here
}
```

Read-only. No write operations.

### Toolkit Interface

Each toolkit constructor returns a `Toolkit`:

```go
type Toolkit interface {
    llm.ToolDispatcher
    Tools() []llm.Tool
}
```

### Merge

`toolkit.Merge(toolkits ...Toolkit) llm.ToolDispatcher` combines dispatchers; panics on duplicate tool names at construction time.

`toolkit.Tools(toolkits ...Toolkit) []llm.Tool` flattens tool lists.

---

## 3. Agent Run Persistence (`llm/store`)

### Goal

Save and resume agent conversation state (`[]llm.Message`) across process boundaries.

### Interface (no deps — lives in `llm/store`)

```go
type RunState struct {
    ID       string
    Messages []llm.Message
    Metadata map[string]string
}

type RunStore interface {
    Load(ctx context.Context, runID string) (*RunState, error) // nil, nil if not found
    Save(ctx context.Context, state *RunState) error
    Delete(ctx context.Context, runID string) error
}
```

### Usage Pattern

```go
state, _ := store.Load(ctx, runID)
if state == nil {
    state = &llmstore.RunState{ID: runID}
}

result, err := llm.AgentLoopWithOptions(ctx, client, llm.GenerateOptions{
    Messages: state.Messages,
    ...
}, dispatcher, loopOpts)

state.Messages = result.Messages
store.Save(ctx, state)
```

### Backends

| Package | Type | Backend | Storage |
|---|---|---|---|
| `llm/store/memory` | subpackage | `memory.New()` | `map[string]*RunState`, safe for tests |
| `llm/store/file` | subpackage | `file.New(dir)` | One JSON file per run ID in `dir` |
| `llm/store/postgres` | submodule | `postgres.New(pool)` | Single `agent_runs` table; messages as JSONB |
| `llm/store/gcs` | submodule | `gcs.New(bucket)` | One object per run: `runs/{id}.json` |
| `llm/store/r2` | submodule | `r2.New(client, bucket)` | Same layout as GCS via S3-compatible API |

#### Postgres Schema

```sql
CREATE TABLE agent_runs (
    id          TEXT PRIMARY KEY,
    messages    JSONB NOT NULL,
    metadata    JSONB,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

---

## 4. Provider Registry (`llm/registry`)

### Goal

`database/sql`-style driver registration: blank-import a provider package to make it available by name string.

### API

```go
// llm/registry/registry.go

type ProviderOptions struct {
    APIKey     string
    BaseURL    string
    HTTPClient *http.Client
}

type ProviderFactory func(modelID string, opts ProviderOptions) (llm.Client, error)

func Register(name string, factory ProviderFactory)
func NewClient(model string, opts ProviderOptions) (llm.Client, error)
// model format: "provider:model-id", e.g. "anthropic:claude-sonnet-4-6"
```

### Provider Adapter Pattern

Each provider ships a thin `adapter` subpackage:

```go
// anthropic/adapter/adapter.go
package adapter

import (
    "github.com/shepard-labs/go-ai-sdk/anthropic"
    "github.com/shepard-labs/go-ai-sdk/llm/registry"
)

func init() {
    registry.Register("anthropic", func(modelID string, opts registry.ProviderOptions) (llm.Client, error) {
        return anthropic.NewClient(opts.APIKey, anthropic.AnthropicModelID(modelID))
    })
}
```

### User-Side Usage

```go
import (
    _ "github.com/shepard-labs/go-ai-sdk/anthropic/adapter"
    _ "github.com/shepard-labs/go-ai-sdk/openai/adapter"
    "github.com/shepard-labs/go-ai-sdk/llm/registry"
)

client, err := registry.NewClient("anthropic:claude-sonnet-4-6", registry.ProviderOptions{
    APIKey: os.Getenv("ANTHROPIC_API_KEY"),
})
```

### Error Handling

`NewClient` returns a descriptive error if the provider name is not registered, prompting the user to blank-import the adapter.

---

## Non-Goals

- Tool approval/human-in-the-loop gates (not requested)
- Per-step tracing or artifact storage (not requested)
- Streaming support for toolkits (out of scope)
- Ollama, Mistral, Hugging Face provider adapters (can be added later)

---

## Open Questions

- Should `llm/store/postgres` reuse the existing `pgxpool` dependency already in `go-dagger`, or be completely independent?
- Should `llm/registry` support `BaseURL` override for OpenAI-compatible providers (Ollama, LM Studio) or defer to `openaicompatible/adapter`?
