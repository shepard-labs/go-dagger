package dag

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/shepard-labs/go-dagger/internal/apperrors"
	"github.com/shepard-labs/go-dagger/pkg/task"
)

type RunState struct{}

func okFunc(context.Context, *RunState) (*RunState, error) {
	return &RunState{}, nil
}

func TestREQDAG001RejectsEmptyDAG(t *testing.T) {
	d := &DAG[RunState]{Name: "empty", Tasks: map[string]*task.Task[RunState]{}}
	if err := d.Validate(); !errors.Is(err, apperrors.ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestREQDAG001RejectsDuplicateTaskNames(t *testing.T) {
	yml := baseYAML() + `
  - name: first
    execute:
      type: go
      function: test.ok
`
	_, err := ParseYAML([]byte(yml), funcs(), nil, nil)
	if !errors.Is(err, apperrors.ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestREQDAG001RejectsMissingDependency(t *testing.T) {
	d := testDAG(taskWithDeps("a", "missing"))
	if err := d.Validate(); !errors.Is(err, apperrors.ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestREQDAG001RejectsSelfDependency(t *testing.T) {
	d := testDAG(taskWithDeps("a", "a"))
	if err := d.Validate(); !errors.Is(err, apperrors.ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestREQDAG001RejectsCycle(t *testing.T) {
	a := taskWithDeps("a", "b")
	b := taskWithDeps("b", "a")
	d := testDAG(a, b)
	err := d.Validate()
	if !errors.Is(err, apperrors.ErrValidation) || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("expected cycle ErrValidation, got %v", err)
	}
}

func TestREQDAG001RejectsInvalidNames(t *testing.T) {
	cases := []*DAG[RunState]{
		{Name: " ", Tasks: map[string]*task.Task[RunState]{"a": taskWithDeps("a")}, TaskOrder: []string{"a"}},
		testDAG(&task.Task[RunState]{Name: "bad.name", Execute: okFunc}),
		testDAG(&task.Task[RunState]{Name: " ", Execute: okFunc}),
	}
	for _, d := range cases {
		if err := d.Validate(); !errors.Is(err, apperrors.ErrValidation) {
			t.Fatalf("expected ErrValidation, got %v", err)
		}
	}
}

func TestREQDAG001RejectsInvalidExecutionMode(t *testing.T) {
	taskA := taskWithDeps("a")
	taskA.Mode = "invalid"
	if err := testDAG(taskA).Validate(); !errors.Is(err, apperrors.ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestREQDAG001RejectsInvalidRetryConfig(t *testing.T) {
	cases := []task.RetryConfig{
		{MaxAttempts: -1},
		{MaxAttempts: 1, BaseDelay: -time.Second},
		{MaxAttempts: 1, MaxDelay: -time.Second},
		{MaxAttempts: 1, BaseDelay: 2 * time.Second, MaxDelay: time.Second},
	}
	for _, retry := range cases {
		taskA := taskWithDeps("a")
		taskA.Retry = retry
		if err := testDAG(taskA).Validate(); !errors.Is(err, apperrors.ErrValidation) {
			t.Fatalf("expected ErrValidation for %+v, got %v", retry, err)
		}
	}
}

func TestREQDAG001RejectsInvalidTimeouts(t *testing.T) {
	d := testDAG(taskWithDeps("a"))
	d.Timeout = -time.Second
	if err := d.Validate(); !errors.Is(err, apperrors.ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
	taskA := taskWithDeps("a")
	taskA.Timeout = -time.Second
	if err := testDAG(taskA).Validate(); !errors.Is(err, apperrors.ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestREQDAG001ValidatesRunStateSerializability(t *testing.T) {
	if err := testDAG(taskWithDeps("a")).Validate(); err != nil {
		t.Fatalf("expected RunState serializability validation to pass, got %v", err)
	}
}

func TestREQDAG002PreservesYAMLTaskOrder(t *testing.T) {
	d := mustParse(t, fullYAML())
	assertEqualStrings(t, d.TaskOrder, []string{"first", "second", "third"})
}

func TestREQDAG002SerializesTasksInTaskOrder(t *testing.T) {
	d := testDAG(taskWithDeps("b"), taskWithDeps("a"))
	d.TaskOrder = []string{"a", "b"}
	for _, taskA := range d.Tasks {
		taskA.FunctionName = "test.ok"
	}
	out, err := SerializeYAML(d)
	if err != nil {
		t.Fatalf("SerializeYAML failed: %v", err)
	}
	if strings.Index(string(out), "name: a") > strings.Index(string(out), "name: b") {
		t.Fatalf("tasks were not serialized in TaskOrder:\n%s", out)
	}
}

func TestREQDAG004BuildsEdgesFromDependsOn(t *testing.T) {
	d := testDAG(taskWithDeps("root"), taskWithDeps("child", "root"))
	if err := d.Validate(); err != nil {
		t.Fatalf("Validate failed: %v", err)
	}
	assertEqualStrings(t, d.Adjacency["root"], []string{"child"})
	if d.InDegree["child"] != 1 || d.InDegree["root"] != 0 {
		t.Fatalf("unexpected in-degree: %#v", d.InDegree)
	}
}

func TestREQDAG004HandlesDuplicateDependencySourcesConsistently(t *testing.T) {
	d := testDAG(taskWithDeps("root"), taskWithDeps("child", "root", "root"))
	if err := d.Validate(); !errors.Is(err, apperrors.ErrValidation) {
		t.Fatalf("expected duplicate DependsOn rejection, got %v", err)
	}
}

func TestREQTASK001ResolvesRegisteredFunction(t *testing.T) {
	d := mustParse(t, baseYAML())
	if d.Tasks["first"].Execute == nil || d.Tasks["first"].FunctionName != "test.ok" {
		t.Fatalf("function was not resolved: %#v", d.Tasks["first"])
	}
}

func TestREQTASK001RejectsMissingFunction(t *testing.T) {
	_, err := ParseYAML([]byte(baseYAML()), task.FunctionRegistry[RunState]{}, nil, nil)
	if !errors.Is(err, apperrors.ErrFunctionNotRegistered) {
		t.Fatalf("expected ErrFunctionNotRegistered, got %v", err)
	}
}

func TestREQTASK001RejectsWrongExecuteType(t *testing.T) {
	yml := strings.Replace(baseYAML(), "type: go", "type: shell", 1)
	_, err := ParseYAML([]byte(yml), funcs(), nil, nil)
	if !errors.Is(err, apperrors.ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestREQTASK001RejectsProgrammaticTaskWithoutExecute(t *testing.T) {
	d := testDAG(&task.Task[RunState]{Name: "a"})
	if err := d.Validate(); !errors.Is(err, apperrors.ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestREQYAML001RejectsUnknownFields(t *testing.T) {
	yml := strings.Replace(baseYAML(), "name: first", "name: first\n    unknown: value", 1)
	_, err := ParseYAML([]byte(yml), funcs(), nil, nil)
	if !errors.Is(err, apperrors.ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestREQYAML001RejectsDuplicateMappingKeys(t *testing.T) {
	yml := "name: one\nname: two\ntasks: []\n"
	_, err := ParseYAML([]byte(yml), funcs(), nil, nil)
	if !errors.Is(err, apperrors.ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestREQYAML001RejectsMissingHookFunctionOrTool(t *testing.T) {
	yml := strings.Replace(baseYAML(), "before_hooks: []", "before_hooks: [missing]", 1)
	_, err := ParseYAML([]byte(yml), funcs(), hooks(), tools())
	if !errors.Is(err, apperrors.ErrValidation) {
		t.Fatalf("expected missing hook ErrValidation, got %v", err)
	}
	yml = strings.Replace(baseYAML(), "tools: []", "tools: [missing]", 1)
	_, err = ParseYAML([]byte(yml), funcs(), hooks(), tools())
	if !errors.Is(err, apperrors.ErrValidation) {
		t.Fatalf("expected missing tool ErrValidation, got %v", err)
	}
}

func TestREQYAML001NormalizesOmittedArraysAndMaps(t *testing.T) {
	yml := `name: pipeline
tasks:
  - name: first
    execute:
      type: go
      function: test.ok
`
	d := mustParse(t, yml)
	taskA := d.Tasks["first"]
	if taskA.Tags == nil || taskA.DependsOn == nil || taskA.Tools == nil || taskA.BeforeHookNames == nil || taskA.AfterHookNames == nil || taskA.ToolNames == nil {
		t.Fatalf("omitted collections were not normalized: %#v", taskA)
	}
}

func TestREQYAML002RoundTripPreservesDeclarationOrder(t *testing.T) {
	d := mustParse(t, fullYAML())
	out, err := SerializeYAML(d)
	if err != nil {
		t.Fatalf("SerializeYAML failed: %v", err)
	}
	roundTrip := mustParse(t, string(out))
	assertEqualStrings(t, roundTrip.TaskOrder, []string{"first", "second", "third"})
	assertEqualStrings(t, roundTrip.Tasks["third"].DependsOn, []string{"first", "second"})
	assertEqualStrings(t, roundTrip.Tasks["first"].BeforeHookNames, []string{"hook"})
	assertEqualStrings(t, roundTrip.Tasks["first"].AfterHookNames, []string{"hook"})
	assertEqualStrings(t, roundTrip.Tasks["first"].ToolNames, []string{"fetch"})
}

func TestREQYAML002SerializeFailsWithoutFunctionName(t *testing.T) {
	d := testDAG(&task.Task[RunState]{Name: "a", Execute: okFunc})
	_, err := SerializeYAML(d)
	if !errors.Is(err, apperrors.ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestREQYAML002SerializeFailsWithoutHookOrToolNames(t *testing.T) {
	d := testDAG(taskWithDeps("a"))
	d.Tasks["a"].FunctionName = "test.ok"
	d.Tasks["a"].BeforeHooks = []task.BeforeHook[RunState]{func(context.Context, *RunState) error { return nil }}
	_, err := SerializeYAML(d)
	if !errors.Is(err, apperrors.ErrValidation) {
		t.Fatalf("expected missing hook name ErrValidation, got %v", err)
	}
	d.Tasks["a"].BeforeHooks = nil
	d.Tasks["a"].Tools = task.ToolRegistry{"fetch": {Name: "fetch"}}
	_, err = SerializeYAML(d)
	if !errors.Is(err, apperrors.ErrValidation) {
		t.Fatalf("expected missing tool name ErrValidation, got %v", err)
	}
}

func TestREQAGENT001ToolNameResolutionBuildsTaskRegistry(t *testing.T) {
	yml := strings.Replace(baseYAML(), "tools: []", "tools: [fetch]", 1)
	d, err := ParseYAML([]byte(yml), funcs(), hooks(), tools())
	if err != nil {
		t.Fatalf("ParseYAML failed: %v", err)
	}
	if _, ok := d.Tasks["first"].Tools["fetch"]; !ok {
		t.Fatalf("task-scoped tool registry missing fetch: %#v", d.Tasks["first"].Tools)
	}
	assertEqualStrings(t, d.Tasks["first"].ToolNames, []string{"fetch"})
}

func testDAG(tasks ...*task.Task[RunState]) *DAG[RunState] {
	d := &DAG[RunState]{Name: "pipeline", Tasks: map[string]*task.Task[RunState]{}, TaskOrder: []string{}}
	for _, taskA := range tasks {
		d.Tasks[taskA.Name] = taskA
		d.TaskOrder = append(d.TaskOrder, taskA.Name)
	}
	return d
}

func taskWithDeps(name string, deps ...string) *task.Task[RunState] {
	return &task.Task[RunState]{Name: name, DependsOn: deps, Execute: okFunc, FunctionName: "test.ok"}
}

func funcs() task.FunctionRegistry[RunState] {
	return task.FunctionRegistry[RunState]{"test.ok": okFunc}
}

func hooks() task.HookRegistry[RunState] {
	return task.HookRegistry[RunState]{"hook": {Before: func(context.Context, *RunState) error { return nil }, After: func(context.Context, *RunState, error) error { return nil }}}
}

func tools() task.ToolRegistry {
	return task.ToolRegistry{"fetch": {Name: "fetch"}}
}

func mustParse(t *testing.T, yml string) *DAG[RunState] {
	t.Helper()
	d, err := ParseYAML([]byte(yml), funcs(), hooks(), tools())
	if err != nil {
		t.Fatalf("ParseYAML failed: %v", err)
	}
	return d
}

func assertEqualStrings(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func baseYAML() string {
	return `name: pipeline
tasks:
  - name: first
    description: First task
    tags:
      env: test
    priority: 1
    execution_mode: parallel
    depends_on: []
    max_attempts: 1
    backoff: none
    before_hooks: []
    after_hooks: []
    tools: []
    execute:
      type: go
      function: test.ok
`
}

func fullYAML() string {
	return `name: pipeline
version: v1
concurrency_limit: 2
tasks:
  - name: first
    description: First task
    tags:
      env: test
    priority: 1
    execution_mode: parallel
    depends_on: []
    max_attempts: 3
    backoff: exponential
    backoff_base: 1s
    backoff_max: 5s
    backoff_jitter: true
    timeout: 10s
    before_hooks: [hook]
    after_hooks: [hook]
    tools: [fetch]
    execute:
      type: go
      function: test.ok
  - name: second
    priority: 2
    execution_mode: parallel
    depends_on: [first]
    max_attempts: 1
    backoff: none
    before_hooks: []
    after_hooks: []
    tools: []
    execute:
      type: go
      function: test.ok
  - name: third
    priority: 3
    execution_mode: sequential
    depends_on: [first, second]
    max_attempts: 1
    backoff: none
    before_hooks: []
    after_hooks: []
    tools: []
    execute:
      type: go
      function: test.ok
`
}
