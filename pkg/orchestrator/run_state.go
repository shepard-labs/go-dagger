package orchestrator

import (
	"encoding/json"
	"fmt"

	"github.com/shepard-labs/go-dagger/internal/apperrors"
)

func copyRunState[S any](state *S) (*S, error) {
	if state == nil {
		state = new(S)
	}
	data, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal run state: %v", apperrors.ErrPersistence, err)
	}
	var copied S
	if err := json.Unmarshal(data, &copied); err != nil {
		return nil, fmt.Errorf("%w: copy run state: %v", apperrors.ErrPersistence, err)
	}
	return &copied, nil
}

func snapshotRunState[S any](state *S) (json.RawMessage, *S, error) {
	copied, err := copyRunState(state)
	if err != nil {
		return nil, nil, err
	}
	snapshot, err := json.Marshal(copied)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: marshal run state snapshot: %v", apperrors.ErrPersistence, err)
	}
	return json.RawMessage(snapshot), copied, nil
}
