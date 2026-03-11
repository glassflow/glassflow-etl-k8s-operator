package pipelinegraph

type NodeType string

const (
	NodeTypeIngestor NodeType = "ingestor"
	NodeTypeDedup    NodeType = "dedup"
	NodeTypeJoin     NodeType = "join"
	NodeTypeSink     NodeType = "sink"
)

type InputType string

const (
	InputTypeIn    InputType = "in"
	InputTypeLeft  InputType = "left"
	InputTypeRight InputType = "right"
)

func (it InputType) String() string {
	return string(it)
}

type Config struct {
	PipelineID string       `json:"pipeline_id"`
	Nodes      []NodeConfig `json:"nodes"`
	Edges      []EdgeConfig `json:"edges"`
}

type NodeConfig struct {
	ID       string   `json:"id"`
	Type     NodeType `json:"type"`
	Replicas int      `json:"replicas"`
}

type EdgeConfig struct {
	ID              string    `json:"id"`
	SourceID        string    `json:"source_id"`
	TargetID        string    `json:"target_id"`
	TargetInputType InputType `json:"target_input_type,omitempty"`
}

type StreamBinding struct {
	Name          string   `json:"name"`
	SubjectPrefix string   `json:"subject_prefix"`
	Subjects      []string `json:"subjects"`
}

type OutputBinding struct {
	Streams []StreamBinding `json:"streams"`
}

type InputBinding struct {
	Streams []StreamBinding `json:"streams"`
}

type JoinInputBinding struct {
	Left  InputBinding `json:"left"`
	Right InputBinding `json:"right"`
}
