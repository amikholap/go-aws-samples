package s3

import (
	"github.com/amikholap/go-aws-samples/global"
)

type Config struct {
	GlobalConfig *global.Config
	Bucket       string
}
