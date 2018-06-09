package s3

import (
	"github.com/amikholap/go-aws-samples/global"
)

type Config struct {
	GlobalConfig *global.Config

	// Working bucket.
	Bucket string

	// Whether to clear the bucket before start.
	Clear bool

	// Number of concurrent workers.
	Concurrency uint

	// Number of iterations per worker.
	NIterations uint

	// Read/Write ratio.
	RWRatio float64
}
