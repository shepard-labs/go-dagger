package dag

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/shepard-labs/go-dagger/internal/apperrors"
	"github.com/shepard-labs/go-dagger/pkg/task"
	"gopkg.in/yaml.v3"
)

type dagYAML struct {
	Name             string     `yaml:"name"`
	Version          string     `yaml:"version,omitempty"`
	ConcurrencyLimit int        `yaml:"concurrency_limit,omitempty"`
	Timeout          string     `yaml:"timeout,omitempty"`
	Tasks            []taskYAML `yaml:"tasks"`
}

type taskYAML struct {
	Name          string            `yaml:"name"`
	Description   string            `yaml:"description,omitempty"`
	Tags          map[string]string `yaml:"tags,omitempty"`
	Priority      int               `yaml:"priority,omitempty"`
	ExecutionMode string            `yaml:"execution_mode,omitempty"`
	DependsOn     []string          `yaml:"depends_on"`
	MaxAttempts   int               `yaml:"max_attempts,omitempty"`
	Backoff       string            `yaml:"backoff,omitempty"`
	BackoffBase   string            `yaml:"backoff_base,omitempty"`
	BackoffMax    string            `yaml:"backoff_max,omitempty"`
	BackoffJitter bool              `yaml:"backoff_jitter,omitempty"`
	Timeout       string            `yaml:"timeout,omitempty"`
	BeforeHooks   []string          `yaml:"before_hooks"`
	AfterHooks    []string          `yaml:"after_hooks"`
	Tools         []string          `yaml:"tools"`
	Execute       executeYAML       `yaml:"execute"`
}

type executeYAML struct {
	Type     string `yaml:"type"`
	Function string `yaml:"function"`
}

// ParseYAML converts a strict YAML DAG definition into a validated DAG.
func ParseYAML[S any](data []byte, functions task.FunctionRegistry[S], hooks task.HookRegistry[S], tools task.ToolRegistry) (*DAG[S], error) {
	var node yaml.Node
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&node); err != nil {
		return nil, fmt.Errorf("%w: parse yaml: %v", apperrors.ErrValidation, err)
	}
	if len(node.Content) == 0 {
		return nil, fmt.Errorf("%w: yaml document is empty", apperrors.ErrValidation)
	}
	if err := rejectDuplicateKeys(node.Content[0], ""); err != nil {
		return nil, err
	}

	var raw dagYAML
	strictDecoder := yaml.NewDecoder(bytes.NewReader(data))
	strictDecoder.KnownFields(true)
	if err := strictDecoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("%w: parse yaml: %v", apperrors.ErrValidation, err)
	}
	d := &DAG[S]{
		Name:             raw.Name,
		Version:          raw.Version,
		ConcurrencyLimit: raw.ConcurrencyLimit,
		Tasks:            make(map[string]*task.Task[S], len(raw.Tasks)),
		TaskOrder:        make([]string, 0, len(raw.Tasks)),
	}
	if raw.Timeout != "" {
		timeout, err := time.ParseDuration(raw.Timeout)
		if err != nil {
			return nil, fmt.Errorf("%w: dag timeout %q is invalid: %v", apperrors.ErrValidation, raw.Timeout, err)
		}
		d.Timeout = timeout
	}
	for i, rawTask := range raw.Tasks {
		t, err := convertYAMLTask(rawTask, functions, hooks, tools)
		if err != nil {
			name := rawTask.Name
			if name == "" {
				name = fmt.Sprintf("index %d", i)
			}
			return nil, fmt.Errorf("task %q: %w", name, err)
		}
		if _, ok := d.Tasks[t.Name]; ok {
			return nil, fmt.Errorf("%w: duplicate task name %q", apperrors.ErrValidation, t.Name)
		}
		d.Tasks[t.Name] = t
		d.TaskOrder = append(d.TaskOrder, t.Name)
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return d, nil
}

// SerializeYAML renders a DAG definition back to the public YAML format.
func SerializeYAML[S any](d *DAG[S]) ([]byte, error) {
	if d == nil {
		return nil, fmt.Errorf("%w: dag is nil", apperrors.ErrValidation)
	}
	order, err := d.normalizedOrder()
	if err != nil {
		return nil, err
	}
	raw := dagYAML{
		Name:             d.Name,
		Version:          d.Version,
		ConcurrencyLimit: d.ConcurrencyLimit,
		Tasks:            make([]taskYAML, 0, len(order)),
	}
	if d.Timeout != 0 {
		raw.Timeout = d.Timeout.String()
	}
	for _, name := range order {
		t := d.Tasks[name]
		if t == nil {
			return nil, fmt.Errorf("%w: task %q is nil", apperrors.ErrValidation, name)
		}
		if t.FunctionName == "" {
			return nil, fmt.Errorf("%w: task %q missing function registry name", apperrors.ErrValidation, name)
		}
		if len(t.BeforeHooks) > 0 && len(t.BeforeHookNames) != len(t.BeforeHooks) {
			return nil, fmt.Errorf("%w: task %q missing before hook registry names", apperrors.ErrValidation, name)
		}
		if len(t.AfterHooks) > 0 && len(t.AfterHookNames) != len(t.AfterHooks) {
			return nil, fmt.Errorf("%w: task %q missing after hook registry names", apperrors.ErrValidation, name)
		}
		if len(t.Tools) > 0 && len(t.ToolNames) != len(t.Tools) {
			return nil, fmt.Errorf("%w: task %q missing tool registry names", apperrors.ErrValidation, name)
		}
		rawTask := taskYAML{
			Name:          t.Name,
			Description:   t.Description,
			Tags:          cloneStringMap(t.Tags),
			Priority:      t.Priority,
			ExecutionMode: string(t.Mode),
			DependsOn:     append([]string(nil), t.DependsOn...),
			MaxAttempts:   t.Retry.MaxAttempts,
			Backoff:       string(t.Retry.Backoff),
			BackoffJitter: t.Retry.Jitter,
			BeforeHooks:   append([]string(nil), t.BeforeHookNames...),
			AfterHooks:    append([]string(nil), t.AfterHookNames...),
			Tools:         append([]string(nil), t.ToolNames...),
			Execute: executeYAML{
				Type:     "go",
				Function: t.FunctionName,
			},
		}
		if t.Retry.BaseDelay != 0 {
			rawTask.BackoffBase = t.Retry.BaseDelay.String()
		}
		if t.Retry.MaxDelay != 0 {
			rawTask.BackoffMax = t.Retry.MaxDelay.String()
		}
		if t.Timeout != 0 {
			rawTask.Timeout = t.Timeout.String()
		}
		raw.Tasks = append(raw.Tasks, rawTask)
	}
	var buf bytes.Buffer
	encoder := yaml.NewEncoder(&buf)
	encoder.SetIndent(2)
	if err := encoder.Encode(raw); err != nil {
		return nil, fmt.Errorf("%w: serialize yaml: %v", apperrors.ErrValidation, err)
	}
	if err := encoder.Close(); err != nil {
		return nil, fmt.Errorf("%w: serialize yaml: %v", apperrors.ErrValidation, err)
	}
	return buf.Bytes(), nil
}

func convertYAMLTask[S any](raw taskYAML, functions task.FunctionRegistry[S], hooks task.HookRegistry[S], tools task.ToolRegistry) (*task.Task[S], error) {
	if raw.Execute.Type == "" {
		return nil, fmt.Errorf("%w: execute.type is required", apperrors.ErrValidation)
	}
	if raw.Execute.Type != "go" {
		return nil, fmt.Errorf("%w: execute.type %q is unsupported", apperrors.ErrValidation, raw.Execute.Type)
	}
	if raw.Execute.Function == "" {
		return nil, fmt.Errorf("%w: execute.function is required", apperrors.ErrFunctionNotRegistered)
	}
	execute, ok := functions[raw.Execute.Function]
	if !ok || execute == nil {
		return nil, fmt.Errorf("%w: function %q", apperrors.ErrFunctionNotRegistered, raw.Execute.Function)
	}
	retry := task.RetryConfig{
		MaxAttempts: raw.MaxAttempts,
		Backoff:     task.BackoffType(raw.Backoff),
		Jitter:      raw.BackoffJitter,
	}
	if raw.BackoffBase != "" {
		duration, err := time.ParseDuration(raw.BackoffBase)
		if err != nil {
			return nil, fmt.Errorf("%w: backoff_base %q is invalid: %v", apperrors.ErrValidation, raw.BackoffBase, err)
		}
		retry.BaseDelay = duration
	}
	if raw.BackoffMax != "" {
		duration, err := time.ParseDuration(raw.BackoffMax)
		if err != nil {
			return nil, fmt.Errorf("%w: backoff_max %q is invalid: %v", apperrors.ErrValidation, raw.BackoffMax, err)
		}
		retry.MaxDelay = duration
	}
	var timeout time.Duration
	if raw.Timeout != "" {
		duration, err := time.ParseDuration(raw.Timeout)
		if err != nil {
			return nil, fmt.Errorf("%w: timeout %q is invalid: %v", apperrors.ErrValidation, raw.Timeout, err)
		}
		timeout = duration
	}
	beforeHooks := make([]task.BeforeHook[S], 0, len(raw.BeforeHooks))
	for _, name := range raw.BeforeHooks {
		hook, ok := hooks[name]
		if !ok || hook.Before == nil {
			return nil, fmt.Errorf("%w: before hook %q", apperrors.ErrValidation, name)
		}
		beforeHooks = append(beforeHooks, hook.Before)
	}
	afterHooks := make([]task.AfterHook[S], 0, len(raw.AfterHooks))
	for _, name := range raw.AfterHooks {
		hook, ok := hooks[name]
		if !ok || hook.After == nil {
			return nil, fmt.Errorf("%w: after hook %q", apperrors.ErrValidation, name)
		}
		afterHooks = append(afterHooks, hook.After)
	}
	taskTools := make(task.ToolRegistry, len(raw.Tools))
	for _, name := range raw.Tools {
		tool, ok := tools[name]
		if !ok {
			return nil, fmt.Errorf("%w: tool %q", apperrors.ErrValidation, name)
		}
		if tool.Name == "" {
			tool.Name = name
		}
		taskTools[name] = tool
	}
	result := &task.Task[S]{
		Name:            raw.Name,
		Description:     raw.Description,
		Tags:            cloneStringMap(raw.Tags),
		Priority:        raw.Priority,
		DependsOn:       append([]string(nil), raw.DependsOn...),
		Mode:            task.ExecutionMode(raw.ExecutionMode),
		Retry:           retry,
		Timeout:         timeout,
		FunctionName:    raw.Execute.Function,
		BeforeHookNames: append([]string(nil), raw.BeforeHooks...),
		AfterHookNames:  append([]string(nil), raw.AfterHooks...),
		ToolNames:       append([]string(nil), raw.Tools...),
		Tools:           taskTools,
		BeforeHooks:     beforeHooks,
		AfterHooks:      afterHooks,
		Execute:         execute,
	}
	result.Normalize()
	return result, nil
}

func rejectDuplicateKeys(node *yaml.Node, path string) error {
	if node == nil {
		return nil
	}
	switch node.Kind {
	case yaml.DocumentNode:
		for _, child := range node.Content {
			if err := rejectDuplicateKeys(child, path); err != nil {
				return err
			}
		}
	case yaml.MappingNode:
		seen := map[string]struct{}{}
		for i := 0; i < len(node.Content); i += 2 {
			key := node.Content[i]
			value := node.Content[i+1]
			keyPath := key.Value
			if path != "" {
				keyPath = path + "." + key.Value
			}
			if _, ok := seen[key.Value]; ok {
				return fmt.Errorf("%w: duplicate yaml key %q", apperrors.ErrValidation, keyPath)
			}
			seen[key.Value] = struct{}{}
			if err := rejectDuplicateKeys(value, keyPath); err != nil {
				return err
			}
		}
	case yaml.SequenceNode:
		for i, child := range node.Content {
			childPath := fmt.Sprintf("%s[%d]", path, i)
			if err := rejectDuplicateKeys(child, strings.TrimPrefix(childPath, "[")); err != nil {
				return err
			}
		}
	}
	return nil
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return map[string]string{}
	}
	output := make(map[string]string, len(input))
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		output[key] = input[key]
	}
	return output
}
