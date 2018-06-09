package s3

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	"log"
	"math/rand"
	"strings"
	"sync"
)

const ALPHABET = "abcdefghijklmnopqrstuvwxyz" + "0123456789"

type WorkerOp int

const (
	WORKER_OP_READ WorkerOp = iota
	WORKER_OP_WRITE
)

type WorkerPool struct {
	Config Config
	Client *s3.S3
}

func (p *WorkerPool) Run() {
	var wg sync.WaitGroup

	log.Println("Pool started")

	for i := 0; i < int(p.Config.Concurrency); i++ {
		w := Worker{
			Config: p.Config,
			Client: p.Client,
		}
		wg.Add(1)
		go w.Run(&wg)
	}
	wg.Wait()

	log.Println("Pool stopped")
}

type Worker struct {
	Config Config
	Client *s3.S3
}

func (w *Worker) Run(wg *sync.WaitGroup) {
	log.Printf("worker %p running\n", w)

	var localWG sync.WaitGroup
	stats := WorkerRuntimeStats{
		CurrentKey: makeRandomString(8),
	}

	downloader := s3manager.NewDownloaderWithClient(w.Client)
	uploader := s3manager.NewUploaderWithClient(w.Client)

	for i := 0; i < int(w.Config.NIterations); i++ {
		switch op := chooseWorkerOp(stats, w.Config.RWRatio); op {
		case WORKER_OP_READ:
			localWG.Add(1)
			stats.IncNReads()
			go func() {
				value := readObject(downloader, w.Config.Bucket, stats.CurrentKey)
				if value == stats.CurrentValue {
					stats.IncNLatestReads()
				} else {
					stats.IncNStaleReads()
				}
				localWG.Done()
			}()
		case WORKER_OP_WRITE:
			localWG.Wait()
			stats.CurrentValue = makeRandomString(64)
			writeObject(uploader, w.Config.Bucket, stats.CurrentKey, stats.CurrentValue)
			stats.IncNWrites()
		default:
			panic(fmt.Sprintf("unknown operation: %s", op))
		}
	}

	localWG.Wait()
	log.Printf("worker %p done\n", w)

	wg.Done()
}

func chooseWorkerOp(stats WorkerRuntimeStats, rwRatio float64) WorkerOp {
	var op WorkerOp
	nWrites := stats.GetNWrites()
	if nWrites == 0 {
		op = WORKER_OP_WRITE
	} else {
		currentRWRatio := float64(stats.GetNReads()) / float64(nWrites)
		if currentRWRatio > rwRatio {
			op = WORKER_OP_WRITE
		} else {
			op = WORKER_OP_READ
		}
	}
	return op
}

func readObject(downloader *s3manager.Downloader, bucket, key string) string {
	input := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}
	buffer := &aws.WriteAtBuffer{}

	_, err := downloader.Download(buffer, input)
	if err != nil {
		panic(err)
	}

	return string(buffer.Bytes())
}

func writeObject(uploader *s3manager.Uploader, bucket, key, value string) string {
	body := strings.NewReader(value)
	input := &s3manager.UploadInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   body,
	}

	_, err := uploader.Upload(input)
	if err != nil {
		panic(err)
	}

	return key
}

func makeRandomString(length int) string {
	buf := make([]byte, length)
	for i := range buf {
		buf[i] = ALPHABET[rand.Intn(len(ALPHABET))]
	}
	return string(buf)
}
