package iface

import (
	"github.com/amikholap/go-aws-samples/global"
)

type Runner interface {
	GetGlobalConfig() global.Config
	Run()
}
