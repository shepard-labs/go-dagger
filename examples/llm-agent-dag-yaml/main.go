package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/shepard-labs/go-ai-sdk/llm"
	"github.com/shepard-labs/go-dagger/pkg/dag"
	"github.com/shepard-labs/go-dagger/pkg/orchestrator"
	"github.com/shepard-labs/go-dagger/pkg/task"
)

const submitResultToolName = "submit_result"

const (
	defaultResearchModel  = llm.AnthropicModelClaudeSonnet46
	defaultRiskModel      = llm.AnthropicModelClaudeSonnet46
	defaultPlanningModel  = llm.AnthropicModelClaudeSonnet46
	defaultCritiqueModel  = llm.AnthropicModelClaudeSonnet46
	defaultSynthesisModel = llm.AnthropicModelClaudeSonnet46
)

type RunState struct {
	Topic         string `json:"topic,omitempty"`
	ResearchBrief string `json:"research_brief,omitempty"`
	RiskBrief     string `json:"risk_brief,omitempty"`
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
	SystemPrompt string
	BuildPrompt  func(*RunState) string
	ApplyResult  func(*RunState, agentResult) error
}

type agentClients struct {
	Research  llm.Client
	Risk      llm.Client
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

	data, err := os.ReadFile(examplePath("dag.yaml"))
	if err != nil {
		return err
	}
	d, err := dag.ParseYAML(data, functions(clients), nil, nil)
	if err != nil {
		return err
	}

	orch, err := orchestrator.NewOrchestrator[RunState](ctx, orchestrator.Config{PostgresDSN: dsn, GlobalTimeout: 3 * time.Minute})
	if err != nil {
		return err
	}
	defer func() { _ = orch.Close() }()

	fmt.Println("loaded YAML DAG", d.Name, "with concurrency limit", d.ConcurrencyLimit)
	run, err := orch.Run(ctx, d, orchestrator.GlobalInputs[RunState]{
		Value: RunState{Topic: "should america block strait of hormuz"},
	})
	if err != nil {
		return err
	}
	fmt.Println("run", run.ID, "finished for", d.Name)
	return nil
}

func functions(clients agentClients) task.FunctionRegistry[RunState] {
	return task.FunctionRegistry[RunState]{
		"examples.llm_agent_dag_yaml.intake":         intake,
		"examples.llm_agent_dag_yaml.research_agent": agentFunc(clients.Research, researchAgent()),
		"examples.llm_agent_dag_yaml.risk_agent":     agentFunc(clients.Risk, riskAgent()),
		"examples.llm_agent_dag_yaml.plan_agent":     agentFunc(clients.Planning, planAgent()),
		"examples.llm_agent_dag_yaml.critique_agent": agentFunc(clients.Critique, critiqueAgent()),
		"examples.llm_agent_dag_yaml.final_agent":    agentFunc(clients.Synthesis, finalAgent()),
	}
}

func intake(ctx context.Context, state *RunState) (*RunState, error) {
	if strings.TrimSpace(state.Topic) == "" {
		return state, fmt.Errorf("topic is required")
	}
	fmt.Println("topic:", state.Topic)
	return state, nil
}

func researchAgent() agentSpec {
	return agentSpec{
		Name:         "research-agent",
		SystemPrompt: "You are a research agent. Extract useful context, constraints, dependencies, and decisions. Always finish by calling submit_result with concise JSON.",
		BuildPrompt: func(state *RunState) string {
			return fmt.Sprintf("Research this topic for an implementation planning DAG: %s", state.Topic)
		},
		ApplyResult: func(state *RunState, result agentResult) error {
			state.ResearchBrief = formatAgentResult(result)
			fmt.Println("research result:", result)
			fmt.Println("research brief:\n" + state.ResearchBrief)
			return nil
		},
	}
}

func riskAgent() agentSpec {
	return agentSpec{
		Name:         "risk-agent",
		SystemPrompt: "You are a risk agent. Identify operational risks, assumptions, rollout concerns, and observability gaps. Always finish by calling submit_result with concise JSON.",
		BuildPrompt: func(state *RunState) string {
			return fmt.Sprintf("Analyze risks for this topic while another agent researches it in parallel: %s", state.Topic)
		},
		ApplyResult: func(state *RunState, result agentResult) error {
			state.RiskBrief = formatAgentResult(result)
			fmt.Println("risk brief:\n" + state.RiskBrief)
			return nil
		},
	}
}

func planAgent() agentSpec {
	return agentSpec{
		Name:         "plan-agent",
		SystemPrompt: "You are a planning agent. Convert research and risk analysis into concrete implementation steps, sequencing, and acceptance criteria. Always finish by calling submit_result with concise JSON.",
		BuildPrompt: func(state *RunState) string {
			return fmt.Sprintf("Topic: %s\n\nResearch brief:\n%s\n\nRisk brief:\n%s\n\nCreate a practical execution plan that accounts for both inputs.", state.Topic, state.ResearchBrief, state.RiskBrief)
		},
		ApplyResult: func(state *RunState, result agentResult) error {
			state.ExecutionPlan = formatAgentResult(result)
			fmt.Println("execution plan:\n" + state.ExecutionPlan)
			return nil
		},
	}
}

func critiqueAgent() agentSpec {
	return agentSpec{
		Name:         "critique-agent",
		SystemPrompt: "You are a critique agent. Find missing constraints, weak assumptions, and sequencing problems. Always finish by calling submit_result with concise JSON.",
		BuildPrompt: func(state *RunState) string {
			return fmt.Sprintf("Topic: %s\n\nPlan:\n%s\n\nRisk brief:\n%s\n\nCritique this plan and identify improvements.", state.Topic, state.ExecutionPlan, state.RiskBrief)
		},
		ApplyResult: func(state *RunState, result agentResult) error {
			state.Critique = formatAgentResult(result)
			fmt.Println("critique:\n" + state.Critique)
			return nil
		},
	}
}

func finalAgent() agentSpec {
	return agentSpec{
		Name:         "final-agent",
		SystemPrompt: "You are a final synthesis agent. Produce the final, actionable answer using the research, risk analysis, plan, and critique. Always finish by calling submit_result with concise JSON.",
		BuildPrompt: func(state *RunState) string {
			return fmt.Sprintf("Topic: %s\n\nResearch brief:\n%s\n\nRisk brief:\n%s\n\nExecution plan:\n%s\n\nCritique:\n%s\n\nCreate the final answer.", state.Topic, state.ResearchBrief, state.RiskBrief, state.ExecutionPlan, state.Critique)
		},
		ApplyResult: func(state *RunState, result agentResult) error {
			state.FinalAnswer = formatAgentResult(result)
			fmt.Println("final answer:\n" + state.FinalAnswer)
			return nil
		},
	}
}

func agentFunc(client llm.Client, spec agentSpec) task.ExecuteFunc[RunState] {
	return func(ctx context.Context, state *RunState) (*RunState, error) {
		result, err := runAgent(ctx, client, spec, state)
		if err != nil {
			fmt.Printf("[%s] API error: %v\n", spec.Name, err)
			return state, err
		}
		if err := spec.ApplyResult(state, result); err != nil {
			return state, err
		}
		return state, nil
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

func newAgentClients(apiKey string) (agentClients, error) {
	factory := newAnthropicClientFactory(apiKey)
	research, err := factory.Client(modelFromEnv("RESEARCH_AGENT_MODEL", defaultResearchModel))
	if err != nil {
		return agentClients{}, err
	}
	risk, err := factory.Client(modelFromEnv("RISK_AGENT_MODEL", defaultRiskModel))
	if err != nil {
		return agentClients{}, err
	}
	planning, err := factory.Client(modelFromEnv("PLANNING_AGENT_MODEL", defaultPlanningModel))
	if err != nil {
		return agentClients{}, err
	}
	critique, err := factory.Client(modelFromEnv("CRITIQUE_AGENT_MODEL", defaultCritiqueModel))
	if err != nil {
		return agentClients{}, err
	}
	synthesis, err := factory.Client(modelFromEnv("SYNTHESIS_AGENT_MODEL", defaultSynthesisModel))
	if err != nil {
		return agentClients{}, err
	}
	return agentClients{Research: research, Risk: risk, Planning: planning, Critique: critique, Synthesis: synthesis}, nil
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

func examplePath(name string) string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return name
	}
	return filepath.Join(filepath.Dir(file), name)
}
