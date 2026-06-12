package task

import (
	"context"
	"encoding/json"
	"testing"
)

func TestREQTOOL002_UnknownToolDispatchError(t *testing.T) {
	_, err := ToolRegistry{}.Dispatch(context.Background(), "missing", nil)
	if err == nil {
		t.Fatal("Dispatch error = nil, want error")
	}
}

func TestREQTOOL002_NilHandlerDispatchError(t *testing.T) {
	_, err := ToolRegistry{"nil": {Name: "nil"}}.Dispatch(context.Background(), "nil", nil)
	if err == nil {
		t.Fatal("Dispatch error = nil, want error")
	}
}

func TestREQTOOL002_SuccessReturnsBytesUnchanged(t *testing.T) {
	want := json.RawMessage(`{"ok":true}`)
	registry := ToolRegistry{"ok": {Name: "ok", Handler: func(context.Context, json.RawMessage) (json.RawMessage, error) { return want, nil }}}
	got, err := registry.Dispatch(context.Background(), "ok", nil)
	if err != nil {
		t.Fatalf("Dispatch error = %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("Dispatch result = %s, want %s", got, want)
	}
}
