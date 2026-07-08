# Examples

This directory contains runnable examples for `go-dagger`. Most examples are
small DAGs that persist run and task state in Postgres; a few are parser,
validation, or inspection utilities.

## Prerequisites

- Go `1.25.0` or newer.
- A Postgres database for examples that run or inspect orchestrator state.
- A `.env` file with `POSTGRES_DSN` for Postgres-backed examples.

Create the shared environment file from the repository root:

```bash
cp .env.example .env
```

Then edit `.env` and set `POSTGRES_DSN` to a reachable database, for example:

```bash
POSTGRES_DSN=postgres://user:secret@localhost:5432/dbname?sslmode=disable
```

Examples load both their local `.env` and the repository root `.env`, so the
root-level file is enough for the standard `go run .` commands below.

## Running An Example

Run examples from their own directory:

```bash
cd examples/minimal
go run .
```

Or run one from the repository root:

```bash
go run ./examples/minimal
```

For YAML-backed examples, keep the working directory as either the example
directory or the repository root command above. Their code resolves `dag.yaml`
relative to the source file.

## Example Index

| Example | Command | Requirements | Notes |
| --- | --- | --- | --- |
| `minimal` | `go run ./examples/minimal` | `POSTGRES_DSN` | Smallest persisted DAG run. |
| `order-fulfillment` | `go run ./examples/order-fulfillment` | `POSTGRES_DSN` | Programmatic DAG with inventory/payment fan-out and fulfillment fan-in. |
| `etl` | `go run ./examples/etl` | `POSTGRES_DSN` | Programmatic extract/transform/load workflow. |
| `incident-response` | `go run ./examples/incident-response` | `POSTGRES_DSN` | SRE incident response workflow with customer communication in parallel. |
| `video-publishing-yaml` | `go run ./examples/video-publishing-yaml` | `POSTGRES_DSN` | YAML DAG with video processing fan-out and per-task timeouts. |
| `humanoid-drone-fusion-yaml` | `go run ./examples/humanoid-drone-fusion-yaml` | `POSTGRES_DSN` | YAML DAG with wide parallel fan-out for multi-robot planning. |
| `public-api-chain-yaml` | `go run ./examples/public-api-chain-yaml` | `POSTGRES_DSN`, internet access | Calls public APIs: `ipapi.co`, `restcountries.com`, `open-meteo.com`, and `sunrise-sunset.org`. |
| `llm-agent-dag` | `go run ./examples/llm-agent-dag` | `POSTGRES_DSN`, `ANTHROPIC_API_KEY` | Programmatic LLM-agent DAG using Anthropic via `go-ai-sdk`. |
| `llm-agent-dag-yaml` | `go run ./examples/llm-agent-dag-yaml` | `POSTGRES_DSN`, `ANTHROPIC_API_KEY` | YAML LLM-agent DAG with research/risk fan-out. |
| `llm-agent-failure` | `go run ./examples/llm-agent-failure` | `POSTGRES_DSN` | Uses mock LLM clients and intentionally demonstrates retry exhaustion; it prints the expected failure and exits successfully. |
| `query` | `DAG_RUN_ID=<run-id> go run ./examples/query` | `POSTGRES_DSN`, `DAG_RUN_ID` | Does not run a DAG. Reads an existing persisted run and task runs. |
| `resume` | `DAG_RUN_ID=<run-id> go run ./examples/resume` | `POSTGRES_DSN`, `DAG_RUN_ID` | Resumes an existing DAG run. Use a run ID from a previous run, often found with `query`. |
| `yaml` | `go run ./examples/yaml` | none | Parses and serializes an inline YAML DAG. Does not use Postgres. |
| `cycle` | `go run ./examples/cycle` | none | Intentionally validates a cyclic DAG and exits with the expected validation error. |

## LLM Agent Examples

`llm-agent-dag` and `llm-agent-dag-yaml` require an Anthropic API key:

```bash
ANTHROPIC_API_KEY=sk-ant-... go run ./examples/llm-agent-dag
```

You can also put `ANTHROPIC_API_KEY` in the root `.env`.

Optional model overrides are supported:

```bash
RESEARCH_AGENT_MODEL=claude-haiku-4-5-20251001 \
PLANNING_AGENT_MODEL=claude-sonnet-4-5 \
CRITIQUE_AGENT_MODEL=claude-sonnet-4-5 \
SYNTHESIS_AGENT_MODEL=claude-sonnet-4-5 \
go run ./examples/llm-agent-dag
```

For `llm-agent-dag-yaml`, `RISK_AGENT_MODEL` is also supported:

```bash
RESEARCH_AGENT_MODEL=claude-haiku-4-5-20251001 \
RISK_AGENT_MODEL=claude-haiku-4-5-20251001 \
PLANNING_AGENT_MODEL=claude-sonnet-4-5 \
CRITIQUE_AGENT_MODEL=claude-sonnet-4-5 \
SYNTHESIS_AGENT_MODEL=claude-sonnet-4-5 \
go run ./examples/llm-agent-dag-yaml
```

`llm-agent-failure` does not call Anthropic. It uses mock clients to show how a
failed agent exhausts retries and skips downstream tasks.

## Querying And Resuming Runs

Many examples print the created run ID when they finish:

```text
run <uuid> finished
```

Use that ID with the read-only query example:

```bash
DAG_RUN_ID=<uuid> go run ./examples/query
```

Use an existing run ID with the resume example:

```bash
DAG_RUN_ID=<uuid> go run ./examples/resume
```

## Per-Example Details

Most example directories have their own `README.md` with the DAG shape,
Mermaid diagram, and any notable configuration for that workflow.
