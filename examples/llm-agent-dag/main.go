package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/shepard-labs/go-dagger/pkg/dag"
	"github.com/shepard-labs/go-dagger/pkg/llm"
	"github.com/shepard-labs/go-dagger/pkg/orchestrator"
	"github.com/shepard-labs/go-dagger/pkg/task"
)

const submitResultToolName = "submit_result"

type RunState struct {
	Topic         string `json:"topic,omitempty"`
	ResearchBrief string `json:"research_brief,omitempty"`
	ExecutionPlan string `json:"execution_plan,omitempty"`
	Critique      string `json:"critique,omitempty"`
	FinalAnswer   string `json:"final_answer,omitempty"`
}

type agentResult struct {
	Summary string   `json:"summary"`
	Bullets []string `json:"bullets,omitempty"`
}

func (r agentResult) String() string {
	return formatAgentResult(r)
}

type agentSpec struct {
	Name         string
	DependsOn    []string
	Description  string
	SystemPrompt string
	BuildPrompt  func(*RunState) string
	ApplyResult  func(*RunState, agentResult) error
}

type agentClients struct {
	Research  llm.Client
	Planning  llm.Client
	Critique  llm.Client
	Synthesis llm.Client
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
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("ANTHROPIC_API_KEY is required")
	}

	clients, err := newAgentClients(apiKey)
	if err != nil {
		return err
	}

	d := agentDAG(clients)
	if err := d.Validate(); err != nil {
		return err
	}

	orch, err := orchestrator.NewOrchestrator[RunState](ctx, orchestrator.Config{PostgresDSN: dsn, GlobalTimeout: 3 * time.Minute})
	if err != nil {
		return err
	}
	defer func() { _ = orch.Close() }()

	run, err := orch.Run(ctx, d, orchestrator.GlobalInputs[RunState]{
		Value: RunState{Topic: "launching an on-call handoff process for a payments platform"},
	})
	if err != nil {
		return err
	}
	fmt.Println("run", run.ID, "finished for", d.Name)
	return nil
}

func newAgentClients(apiKey string) (agentClients, error) {
	factory := newAnthropicClientFactory(apiKey)
	research, err := factory.Client(modelFromEnv("RESEARCH_AGENT_MODEL", llm.AnthropicModelClaudeSonnet46))
	if err != nil {
		return agentClients{}, err
	}
	planning, err := factory.Client(modelFromEnv("PLANNING_AGENT_MODEL", llm.AnthropicModelClaudeSonnet46))
	if err != nil {
		return agentClients{}, err
	}
	critique, err := factory.Client(modelFromEnv("CRITIQUE_AGENT_MODEL", llm.AnthropicModelClaudeSonnet46))
	if err != nil {
		return agentClients{}, err
	}
	synthesis, err := factory.Client(modelFromEnv("SYNTHESIS_AGENT_MODEL", llm.AnthropicModelClaudeSonnet46))
	if err != nil {
		return agentClients{}, err
	}
	return agentClients{Research: research, Planning: planning, Critique: critique, Synthesis: synthesis}, nil
}

type anthropicClientFactory struct {
	apiKey  string
	clients map[llm.AnthropicModelID]llm.Client
}

func newAnthropicClientFactory(apiKey string) *anthropicClientFactory {
	return &anthropicClientFactory{apiKey: apiKey, clients: map[llm.AnthropicModelID]llm.Client{}}
}

func (f *anthropicClientFactory) Client(modelID llm.AnthropicModelID) (llm.Client, error) {
	if client, ok := f.clients[modelID]; ok {
		return client, nil
	}
	client, err := llm.NewAnthropicClient(f.apiKey, modelID)
	if err != nil {
		return nil, err
	}
	f.clients[modelID] = client
	return client, nil
}

func modelFromEnv(name string, fallback llm.AnthropicModelID) llm.AnthropicModelID {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return llm.AnthropicModelID(value)
	}
	return fallback
}

func agentDAG(clients agentClients) *dag.DAG[RunState] {
	d := &dag.DAG[RunState]{
		Name:             "llm-agent-dag-example",
		ConcurrencyLimit: 2,
		TaskOrder:        []string{"intake", "research-agent", "plan-agent", "critique-agent", "final-agent"},
		Tasks:            map[string]*task.Task[RunState]{},
	}

	d.Tasks["intake"] = &task.Task[RunState]{
		Name:         "intake",
		Description:  "Validate the initial topic before the agent DAG starts.",
		FunctionName: "examples.llm_agent_dag.intake",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			if strings.TrimSpace(state.Topic) == "" {
				return state, fmt.Errorf("topic is required")
			}
			fmt.Println("topic:", state.Topic)
			return state, nil
		},
	}

	d.Tasks["research-agent"] = agentTask(clients.Research, agentSpec{
		Name:         "research-agent",
		DependsOn:    []string{"intake"},
		Description:  "Research the topic and produce a concise brief.",
		SystemPrompt: "You are a research agent. Extract useful context, risks, constraints, and decisions. Always finish by calling submit_result with concise JSON.",
		BuildPrompt: func(state *RunState) string {
			return fmt.Sprintf("Research this topic for an implementation planning DAG: %s", state.Topic)
		},
		ApplyResult: func(state *RunState, result agentResult) error {
			state.ResearchBrief = formatAgentResult(result)
			fmt.Println("research brief:\n" + state.ResearchBrief)
			return nil
		},
	})

	d.Tasks["plan-agent"] = agentTask(clients.Planning, agentSpec{
		Name:         "plan-agent",
		DependsOn:    []string{"research-agent"},
		Description:  "Turn the research brief into an execution plan.",
		SystemPrompt: "You are a planning agent. Convert research into concrete implementation steps, sequencing, and acceptance criteria. Always finish by calling submit_result with concise JSON.",
		BuildPrompt: func(state *RunState) string {
			return fmt.Sprintf("Topic: %s\n\nResearch brief:\n%s\n\nCreate a practical execution plan.", state.Topic, state.ResearchBrief)
		},
		ApplyResult: func(state *RunState, result agentResult) error {
			state.ExecutionPlan = formatAgentResult(result)
			fmt.Println("execution plan:\n" + state.ExecutionPlan)
			return nil
		},
	})

	d.Tasks["critique-agent"] = agentTask(clients.Critique, agentSpec{
		Name:         "critique-agent",
		DependsOn:    []string{"plan-agent"},
		Description:  "Review the plan for gaps before final synthesis.",
		SystemPrompt: "You are a critique agent. Find missing constraints, weak assumptions, and sequencing problems. Always finish by calling submit_result with concise JSON.",
		BuildPrompt: func(state *RunState) string {
			return fmt.Sprintf("Topic: %s\n\nPlan:\n%s\n\nCritique this plan and identify improvements.", state.Topic, state.ExecutionPlan)
		},
		ApplyResult: func(state *RunState, result agentResult) error {
			state.Critique = formatAgentResult(result)
			fmt.Println("critique:\n" + state.Critique)
			return nil
		},
	})

	d.Tasks["final-agent"] = agentTask(clients.Synthesis, agentSpec{
		Name:         "final-agent",
		DependsOn:    []string{"critique-agent"},
		Description:  "Synthesize the final answer from all previous agent outputs.",
		SystemPrompt: "You are a final synthesis agent. Produce the final, actionable answer using the research, plan, and critique. Always finish by calling submit_result with concise JSON.",
		BuildPrompt: func(state *RunState) string {
			return fmt.Sprintf("Topic: %s\n\nResearch brief:\n%s\n\nExecution plan:\n%s\n\nCritique:\n%s\n\nCreate the final answer.", state.Topic, state.ResearchBrief, state.ExecutionPlan, state.Critique)
		},
		ApplyResult: func(state *RunState, result agentResult) error {
			state.FinalAnswer = formatAgentResult(result)
			fmt.Println("final answer:\n" + state.FinalAnswer)
			return nil
		},
	})

	return d
}

func agentTask(client llm.Client, spec agentSpec) *task.Task[RunState] {
	return &task.Task[RunState]{
		Name:         spec.Name,
		Description:  spec.Description,
		DependsOn:    spec.DependsOn,
		Timeout:      time.Minute,
		FunctionName: "examples.llm_agent_dag." + strings.ReplaceAll(spec.Name, "-", "_"),
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			result, err := runAgent(ctx, client, spec, state)
			if err != nil {
				return state, err
			}
			if err := spec.ApplyResult(state, result); err != nil {
				return state, err
			}
			return state, nil
		},
	}
}

func runAgent(ctx context.Context, client llm.Client, spec agentSpec, state *RunState) (agentResult, error) {
	_, raw, err := llm.AgentLoopWithOptions(ctx, client, llm.GenerateOptions{
		System:    spec.SystemPrompt,
		MaxTokens: 1024,
		Messages: []llm.Message{{
			Role:    "user",
			Content: []llm.Content{llm.TextContent{Text: agentPrompt(spec, state)}},
		}},
		Tools: []llm.Tool{submitResultTool()},
	}, task.ToolRegistry{}, llm.AgentLoopOptions{
		MaxTurns:       6,
		MaxToolRepairs: 2,
		ToolPolicies: map[string]llm.ToolPolicy{
			submitResultToolName: {Terminal: true, Validate: validateAgentResultInput},
		},
	})
	if err != nil {
		return agentResult{}, err
	}

	var result agentResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return agentResult{}, err
	}
	if strings.TrimSpace(result.Summary) == "" {
		return agentResult{}, fmt.Errorf("%s returned empty summary: %s", spec.Name, string(raw))
	}
	if !hasNonEmptyBullet(result.Bullets) {
		return agentResult{}, fmt.Errorf("%s returned no supporting bullets: %s", spec.Name, string(raw))
	}
	return result, nil
}

func agentPrompt(spec agentSpec, state *RunState) string {
	return spec.BuildPrompt(state) + `

Return your answer only by calling submit_result with this exact shape:
{
  "summary": "one sentence summary",
  "bullets": ["specific detail 1", "specific detail 2", "specific detail 3"]
}

The bullets array is required and must contain at least 3 concrete, non-empty items.`
}

func submitResultTool() llm.Tool {
	return llm.Tool{
		Name:        submitResultToolName,
		Description: "Submit the agent's structured result and end this agent turn. Include concrete supporting bullets, not just a one-line summary.",
		InputSchema: json.RawMessage(`{
			"type":"object",
			"required":["summary","bullets"],
			"properties":{
				"summary":{"type":"string","description":"A concise result summary."},
				"bullets":{"type":"array","minItems":3,"items":{"type":"string"},"description":"Concrete supporting details, decisions, risks, or steps."}
			}
		}`),
	}
}

func validateAgentResultInput(raw json.RawMessage) error {
	var result agentResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return err
	}
	if strings.TrimSpace(result.Summary) == "" {
		return fmt.Errorf("summary is required")
	}
	if !hasNonEmptyBullet(result.Bullets) {
		return fmt.Errorf("bullets must include at least one non-empty item")
	}
	return nil
}

func hasNonEmptyBullet(bullets []string) bool {
	for _, bullet := range bullets {
		if strings.TrimSpace(bullet) != "" {
			return true
		}
	}
	return false
}

func formatAgentResult(result agentResult) string {
	if len(result.Bullets) == 0 {
		return result.Summary
	}
	lines := []string{result.Summary}
	for _, bullet := range result.Bullets {
		if strings.TrimSpace(bullet) != "" {
			lines = append(lines, "- "+bullet)
		}
	}
	return strings.Join(lines, "\n")
}
