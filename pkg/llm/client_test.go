package llm

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

func TestREQLLM001_ClientOnlyGenerate(t *testing.T) {
	clientType := reflect.TypeOf((*Client)(nil)).Elem()
	if clientType.NumMethod() != 1 {
		t.Fatalf("Client has %d methods, want 1", clientType.NumMethod())
	}
	method, ok := clientType.MethodByName("Generate")
	if !ok {
		t.Fatal("Client missing Generate method")
	}
	want := reflect.TypeOf(func(context.Context, GenerateOptions) (*GenerateResult, error) { return nil, nil })
	if method.Type.NumIn() != want.NumIn() || method.Type.NumOut() != want.NumOut() {
		t.Fatalf("Generate signature = %s, want Client.Generate(context.Context, GenerateOptions) (*GenerateResult, error)", method.Type)
	}
	for i := 0; i < want.NumIn(); i++ {
		if method.Type.In(i) != want.In(i) {
			t.Fatalf("Generate input %d = %s, want %s", i, method.Type.In(i), want.In(i))
		}
	}
	for i := 0; i < want.NumOut(); i++ {
		if method.Type.Out(i) != want.Out(i) {
			t.Fatalf("Generate output %d = %s, want %s", i, method.Type.Out(i), want.Out(i))
		}
	}
}

func TestREQLLM003_PublicTypesNoGoAISDKReexport(t *testing.T) {
	types := []reflect.Type{
		reflect.TypeOf(Message{}),
		reflect.TypeOf(TextContent{}),
		reflect.TypeOf(ToolUseContent{}),
		reflect.TypeOf(ToolResultContent{}),
		reflect.TypeOf(Tool{}),
		reflect.TypeOf(GenerateResult{}),
		reflect.TypeOf(Usage{}),
	}
	for _, typ := range types {
		if typ.PkgPath() != "github.com/shepard-labs/go-dagger/pkg/llm" {
			t.Fatalf("%s package = %q", typ.Name(), typ.PkgPath())
		}
	}

	var contents []Content = []Content{
		TextContent{Text: "hello"},
		ToolUseContent{ID: "call", Name: "tool", Input: json.RawMessage(`{}`)},
		ToolResultContent{ToolUseID: "call", Text: "ok"},
	}
	if len(contents) != 3 {
		t.Fatalf("content implementations = %d, want 3", len(contents))
	}
}

func TestREQLLM004_ToolInputSchemaRawMessage(t *testing.T) {
	field, ok := reflect.TypeOf(Tool{}).FieldByName("InputSchema")
	if !ok {
		t.Fatal("Tool missing InputSchema")
	}
	if field.Type != reflect.TypeOf(json.RawMessage{}) {
		t.Fatalf("Tool.InputSchema type = %s, want json.RawMessage", field.Type)
	}
}
