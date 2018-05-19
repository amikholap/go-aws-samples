package cmd

import (
	"fmt"
	"github.com/amikholap/go-aws-samples/s3"
	"github.com/spf13/cobra"
)

var s3Config = s3.Config{
	GlobalConfig: globalConfig,
}

func init() {
	cobra.OnInitialize()

	s3Cmd.PersistentFlags().StringVar(&s3Config.Bucket, "bucket", "", "S3 Bucket")
	s3Cmd.MarkPersistentFlagRequired("bucket")

	rootCmd.AddCommand(s3Cmd)
}

var s3Cmd = &cobra.Command{
	Use:   "s3",
	Short: "Run S3 scenario",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("S3 RUNNING")
		fmt.Printf("%q\n", s3Config)

		runner := s3.Runner{
			Config: s3Config,
		}
		runner.Run()
	},
}
