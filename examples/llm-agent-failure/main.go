package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/shepard-labs/go-ai-sdk/llm"
	"github.com/shepard-labs/go-dagger/pkg/dag"
	"github.com/shepard-labs/go-dagger/pkg/orchestrator"
	"github.com/shepard-labs/go-dagger/pkg/task"
)

type RunState struct {
	Topic         string `json:"topic,omitempty"`
	ResearchBrief string `json:"research_brief,omitempty"`
	ExecutionPlan string `json:"execution_plan,omitempty"`
	Critique      string `json:"critique,omitempty"`
	FinalAnswer   string `json:"final_answer,omitempty"`
}

type mockAgentResult struct {
	Summary string `json:"summary"`
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	_ = godotenv.Load(".env")
	_ = godotenv.Load("../../.env")

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		return fmt.Errorf("POSTGRES_DSN is required")
	}

	d := agentDAG()
	if err := d.Validate(); err != nil {
		return err
	}

	orch, err := orchestrator.NewOrchestrator[RunState](ctx, orchestrator.Config{PostgresDSN: dsn, GlobalTimeout: 30 * time.Second})
	if err != nil {
		return err
	}
	defer func() { _ = orch.Close() }()

	fmt.Println("=== starting DAG run: " + d.Name + " ===")
	fmt.Println("pipeline: research-agent → plan-agent → critique-agent → final-agent")
	fmt.Println("the critique-agent will simulate 3 failed API calls and exhaust retries")
	fmt.Println()

	run, err := orch.Run(ctx, d, orchestrator.GlobalInputs[RunState]{
		Value: RunState{Topic: "launching an on-call handoff process"},
	})
	if err != nil {
		fmt.Println()
		fmt.Println("=== run result ===")
		fmt.Printf("run failed: %v\n", err)
		fmt.Println("downstream task 'final-agent' was skipped because 'critique-agent' failed")
		return nil
	}
	fmt.Println("run", run.ID, "finished for", d.Name)
	return nil
}

func agentDAG() *dag.DAG[RunState] {
	successClient := llm.GeneratorFunc(func(_ context.Context, _ llm.GenerateOptions) (*llm.GenerateResult, error) {
		return &llm.GenerateResult{
			FinishReason: llm.FinishReasonStop,
			Content: []llm.Content{llm.TextContent{
				Text: `{"summary":"mock result from agent"}`,
			}},
		}, nil
	})

	failingClient := llm.GeneratorFunc(func(_ context.Context, _ llm.GenerateOptions) (*llm.GenerateResult, error) {
		return nil, fmt.Errorf("API error 500: upstream LLM service unavailable")
	})

	d := &dag.DAG[RunState]{
		Name:             "llm-agent-failure-example",
		ConcurrencyLimit: 2,
		TaskOrder:        []string{"intake", "research-agent", "plan-agent", "critique-agent", "final-agent"},
		Tasks:            map[string]*task.Task[RunState]{},
	}

	d.Tasks["intake"] = &task.Task[RunState]{
		Name:         "intake",
		FunctionName: "examples.llm_agent_failure.intake",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			fmt.Println("[intake] topic:", state.Topic)
			return state, nil
		},
	}

	d.Tasks["research-agent"] = &task.Task[RunState]{
		Name:         "research-agent",
		DependsOn:    []string{"intake"},
		FunctionName: "examples.llm_agent_failure.research",
		Execute:      buildAgentExecute(successClient, "[research-agent] LLM call succeeded", func(state *RunState, result mockAgentResult) { state.ResearchBrief = result.Summary }),
	}

	d.Tasks["plan-agent"] = &task.Task[RunState]{
		Name:         "plan-agent",
		DependsOn:    []string{"research-agent"},
		FunctionName: "examples.llm_agent_failure.plan",
		Execute:      buildAgentExecute(successClient, "[plan-agent] LLM call succeeded", func(state *RunState, result mockAgentResult) { state.ExecutionPlan = result.Summary }),
	}

	d.Tasks["critique-agent"] = &task.Task[RunState]{
		Name:         "critique-agent",
		DependsOn:    []string{"plan-agent"},
		FunctionName: "examples.llm_agent_failure.critique",
		Retry:        task.RetryConfig{MaxAttempts: 3, Backoff: task.BackoffNone},
		Execute:      buildAgentExecute(failingClient, "[critique-agent] LLM call FAILED", nil),
	}

	d.Tasks["final-agent"] = &task.Task[RunState]{
		Name:         "final-agent",
		DependsOn:    []string{"critique-agent"},
		FunctionName: "examples.llm_agent_failure.final",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			state.FinalAnswer = strings.Join([]string{
				"Research: " + state.ResearchBrief,
				"Plan: " + state.ExecutionPlan,
			}, "\n")
			fmt.Println("[final-agent] synthesized final answer (should NOT execute)")
			return state, nil
		},
	}

	return d
}

func buildAgentExecute(client llm.Client, label string, onSuccess func(*RunState, mockAgentResult)) task.ExecuteFunc[RunState] {
	return func(ctx context.Context, state *RunState) (*RunState, error) {
		_, err := client.Generate(ctx, llm.GenerateOptions{})
		if err != nil {
			fmt.Printf("%s (%v)\n", label, err)
			return state, err
		}
		fmt.Println(label)
		if onSuccess != nil {
			onSuccess(state, mockAgentResult{Summary: "mock agent result"})
		}
		return state, nil
	}
}
