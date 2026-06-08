package main

import (
	"context"
	"fmt"
	"github.com/shepard-labs/go-dagger/pkg/dag"
	"github.com/shepard-labs/go-dagger/pkg/task"
	"log"
)

type RunState struct {
	TargetURL string `json:"target_url,omitempty"`
}

func main() {
	functions := task.FunctionRegistry[RunState]{
		"examples.fetch": func(ctx context.Context, state *RunState) (*RunState, error) {
			state.TargetURL = "https://example.com"
			return state, nil
		},
	}
	data := []byte(`name: yaml-example
tasks:
  - name: fetch
    execution_mode: parallel
    depends_on: []
    execute:
      type: go
      function: examples.fetch
`)
	d, err := dag.ParseYAML(data, functions, nil, nil)
	if err != nil {
		log.Fatal(err)
	}
	out, err := dag.SerializeYAML(d)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(string(out))
}
