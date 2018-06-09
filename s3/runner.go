package s3

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	"log"
	"sync"
)

const DeleteBatchSize = 256

type Runner struct {
	Config Config
}

func (r *Runner) Run() {
	client := r.MakeClient()

	if r.Config.Clear {
		log.Printf("clearing bucket %s\n", r.Config.Bucket)
		clearBucket(client, r.Config.Bucket)
		log.Printf("done clearing bucket %s\n", r.Config.Bucket)
	}

	pool := WorkerPool{
		Config: r.Config,
		Client: client,
	}
	pool.Run()
}

func (r *Runner) MakeClient() *s3.S3 {
	var configs []external.Config
	if r.Config.GlobalConfig.Region != "" {
		configs = append(configs, external.WithRegion(r.Config.GlobalConfig.Region))
	}

	awsConfig, err := external.LoadDefaultAWSConfig(configs...)
	if err != nil {
		panic(err)
	}

	client := s3.New(awsConfig)

	return client
}

func clearBucket(client *s3.S3, bucket string) {
	var wg sync.WaitGroup
	var objectsToDelete []s3manager.BatchDeleteObject

	listObjectsInput := s3.ListObjectsInput{
		Bucket: &bucket,
	}
	listObjectsRequest := client.ListObjectsRequest(&listObjectsInput)

	paginator := listObjectsRequest.Paginate()
	for paginator.Next() {
		page := paginator.CurrentPage()
		for _, obj := range page.Contents {
			objectsToDelete = append(objectsToDelete, s3manager.BatchDeleteObject{
				Object: &s3.DeleteObjectInput{
					Key:    obj.Key,
					Bucket: &bucket,
				},
			})
			if len(objectsToDelete) >= DeleteBatchSize {
				wg.Add(1)
				go func() {
					deleteObjects(client, objectsToDelete)
					wg.Done()
				}()
				objectsToDelete = make([]s3manager.BatchDeleteObject, 0, 8)
			}
		}
	}

	if len(objectsToDelete) > 0 {
		deleteObjects(client, objectsToDelete)
	}

	wg.Wait()
}

func deleteObjects(client *s3.S3, objects []s3manager.BatchDeleteObject) {
	deleter := s3manager.NewBatchDeleteWithClient(client)
	deleteObjectsIterator := s3manager.DeleteObjectsIterator{
		Objects: objects,
	}
	if err := deleter.Delete(aws.BackgroundContext(), &deleteObjectsIterator); err != nil {
		panic(err)
	}
}
