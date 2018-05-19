package s3

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Runner struct {
	Config Config
}

func (r *Runner) Run() {
	var configs []external.Config
	if r.Config.GlobalConfig.Region != "" {
		configs = append(configs, external.WithRegion(r.Config.GlobalConfig.Region))
	}

	awsConfig, err := external.LoadDefaultAWSConfig(configs...)
	if err != nil {
		panic(err)
	}

	client := s3.New(awsConfig)
	fmt.Printf("%v\n", client)

	fmt.Printf("listing bucket '%v'\n", r.Config.Bucket)
	input := s3.ListObjectsInput{
		Bucket: &r.Config.Bucket,
	}
	req := client.ListObjectsRequest(&input)

	p := req.Paginate()
	for p.Next() {
		page := p.CurrentPage()
		for _, obj := range page.Contents {
			fmt.Println(obj.Key)
		}
	}

	if err := p.Err(); err != nil {
		panic(err)
	}

	fmt.Println("DONE")
}
