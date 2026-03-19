package models

type ComponentSignal struct {
	PipelineID string `json:"pipeline_id"`
	Reason     string `json:"reason"`
	Text       string `json:"text"`
	Component  string `json:"component"`
}
