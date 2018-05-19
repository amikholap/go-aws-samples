package cmd

import (
	"fmt"
	"github.com/amikholap/go-aws-samples/global"
	"github.com/spf13/cobra"
	"os"
)

var globalConfig = &global.Config{}

var rootCmd = &cobra.Command{
	Use:  "go-aws-samples",
	Args: cobra.ExactArgs(1),
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize()
	rootCmd.PersistentFlags().StringVar(&globalConfig.Region, "region", "", "AWS Region")
}
