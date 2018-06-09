package cmd

import (
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

	s3Cmd.PersistentFlags().BoolVar(&s3Config.Clear, "clear", false, "Whether to clear the bucket before start")
	s3Cmd.PersistentFlags().UintVar(&s3Config.Concurrency, "concurrency", 1, "Number of concurrent workers")
	s3Cmd.PersistentFlags().UintVar(&s3Config.NIterations, "niter", 100, "Number of iterations per worker")
	s3Cmd.PersistentFlags().Float64Var(&s3Config.RWRatio, "rwratio", 1, "Read/write ratio")

	rootCmd.AddCommand(s3Cmd)
}

var s3Cmd = &cobra.Command{
	Use:   "s3",
	Short: "Run S3 scenario",
	Run: func(cmd *cobra.Command, args []string) {
		runner := s3.Runner{
			Config: s3Config,
		}
		runner.Run()
	},
}
