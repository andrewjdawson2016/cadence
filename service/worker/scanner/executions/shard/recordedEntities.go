package shard

import (
	"encoding/json"

	"github.com/uber/cadence/service/worker/scanner/executions/checks"
)

type (
	ScannedRecordedEntity struct {
		CheckRequest *checks.CheckRequest
		CheckResponse *checks.CheckResponse
	}

	CleanedRecordedEntity struct {
		ScannedRecordedEntity *ScannedRecordedEntity

	}
)

func SerializeScannedExecution(se *ScannedRecordedEntity) ([]byte, error) {
	return json.Marshal(se)
}

func DeserializeScannedExecution(data []byte) (*ScannedRecordedEntity, error) {
	var se ScannedRecordedEntity
	if err := json.Unmarshal(data, &se); err != nil {
		return nil, err
	}
	return &se, nil
}
