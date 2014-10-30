package common

import (

)

type PipelineFactory interface {
	NewPipeline (topic string) (Pipeline, error)
}

