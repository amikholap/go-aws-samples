package s3

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
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
	nWorkers := int(p.Config.Concurrency)
	workers := make([]*Worker, nWorkers)

	log.Println("Pool started")

	for i := 0; i < nWorkers; i++ {
		w := NewWorker(p.Config, p.Client)
		workers[i] = w
		wg.Add(1)
		go func() {
			w.Run()
			wg.Done()
		}()
	}
	wg.Wait()

	log.Println("Pool stopped")

	stats := make([]*WorkerRuntimeStats, nWorkers)
	for i := 0; i < nWorkers; i++ {
		stats[i] = workers[i].Stats
	}
	mergedStats := MergeWorkerRuntimeStats(stats...)

	log.Println("Runtime stats:")
	mergedStats.WriteReport(os.Stdout)
}

type Worker struct {
	Config Config
	Client *s3.S3
	Stats  *WorkerRuntimeStats
}

func NewWorker(config Config, client *s3.S3) *Worker {
	return &Worker{
		Config: config,
		Client: client,
		Stats:  NewWorkerRuntimeStats(),
	}
}

func (w *Worker) Run() {
	log.Printf("worker %p running\n", w)

	var wg sync.WaitGroup
	w.Stats.CurrentKey = makeRandomString(8)

	downloader := s3manager.NewDownloaderWithClient(w.Client)
	uploader := s3manager.NewUploaderWithClient(w.Client)

	for i := 0; i < int(w.Config.NIterations); i++ {
		switch op := chooseWorkerOp(w.Stats, w.Config.RWRatio); op {
		case WORKER_OP_READ:
			wg.Add(1)
			w.Stats.IncNReads()
			go func() {
				readStart := time.Now()
				value := readObject(downloader, w.Config.Bucket, w.Stats.CurrentKey)
				w.Stats.AddReadTiming(time.Since(readStart))
				if value == w.Stats.CurrentValue {
					w.Stats.IncNLatestReads()
				} else {
					w.Stats.IncNStaleReads()
				}
				wg.Done()
			}()
		case WORKER_OP_WRITE:
			log.Println("write")
			wg.Wait()
			w.Stats.CurrentValue = makeRandomString(64)
			writeStart := time.Now()
			writeObject(uploader, w.Config.Bucket, w.Stats.CurrentKey, w.Stats.CurrentValue)
			w.Stats.AddWriteTiming(time.Since(writeStart))
			w.Stats.IncNWrites()
		default:
			panic(fmt.Sprintf("unknown operation: %s", op))
		}
	}

	wg.Wait()
	log.Printf("worker %p done\n", w)
}

func chooseWorkerOp(stats *WorkerRuntimeStats, rwRatio float64) WorkerOp {
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
