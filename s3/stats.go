package s3

import (
	"fmt"
	"github.com/jamiealquiza/tachymeter"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const TimingsTachymeterSize = 128

type WorkerRuntimeStats struct {
	CurrentKey   string
	CurrentValue string

	nReads       uint32
	nWrites      uint32
	nLatestReads uint32
	nStaleReads  uint32
	readTimings  []time.Duration
	writeTimings []time.Duration

	m sync.Mutex
}

func NewWorkerRuntimeStats() *WorkerRuntimeStats {
	return &WorkerRuntimeStats{}
}

func (s *WorkerRuntimeStats) GetNReads() uint32 {
	return atomic.LoadUint32(&s.nReads)
}

func (s *WorkerRuntimeStats) IncNReads() {
	atomic.AddUint32(&s.nReads, 1)
}

func (s *WorkerRuntimeStats) GetNWrites() uint32 {
	return atomic.LoadUint32(&s.nWrites)
}

func (s *WorkerRuntimeStats) IncNWrites() {
	atomic.AddUint32(&s.nWrites, 1)
}

func (s *WorkerRuntimeStats) GetNLatestReads() uint32 {
	return atomic.LoadUint32(&s.nLatestReads)
}

func (s *WorkerRuntimeStats) IncNLatestReads() {
	atomic.AddUint32(&s.nLatestReads, 1)
}

func (s *WorkerRuntimeStats) GetNStaleReads() uint32 {
	return atomic.LoadUint32(&s.nStaleReads)
}

func (s *WorkerRuntimeStats) IncNStaleReads() {
	atomic.AddUint32(&s.nStaleReads, 1)
}

func (s *WorkerRuntimeStats) GetReadTimings() []time.Duration {
	return s.readTimings
}

func (s *WorkerRuntimeStats) AddReadTiming(t time.Duration) {
	s.m.Lock()
	defer s.m.Unlock()
	s.readTimings = append(s.readTimings, t)
}

func (s *WorkerRuntimeStats) GetWriteTimings() []time.Duration {
	return s.writeTimings
}

func (s *WorkerRuntimeStats) AddWriteTiming(t time.Duration) {
	s.m.Lock()
	defer s.m.Unlock()
	s.writeTimings = append(s.writeTimings, t)
}

type WorkerMergedStats struct {
	NReads       uint32
	NWrites      uint32
	NLatestReads uint32
	NStaleReads  uint32
	ReadTimings  *tachymeter.Tachymeter
	WriteTimings *tachymeter.Tachymeter
}

func NewWorkerMergedStats() *WorkerMergedStats {
	return &WorkerMergedStats{
		ReadTimings:  tachymeter.New(&tachymeter.Config{Size: TimingsTachymeterSize}),
		WriteTimings: tachymeter.New(&tachymeter.Config{Size: TimingsTachymeterSize}),
	}
}

func MergeWorkerRuntimeStats(stats ...*WorkerRuntimeStats) *WorkerMergedStats {
	mergedStats := NewWorkerMergedStats()

	for _, s := range stats {
		mergedStats.NReads += s.GetNReads()
		mergedStats.NWrites += s.GetNWrites()
		mergedStats.NLatestReads += s.GetNLatestReads()
		mergedStats.NStaleReads += s.GetNStaleReads()

		for _, t := range s.GetReadTimings() {
			mergedStats.ReadTimings.AddTime(t)
		}
		for _, t := range s.GetWriteTimings() {
			mergedStats.WriteTimings.AddTime(t)
		}
	}

	return mergedStats
}

func (s *WorkerMergedStats) WriteReport(out io.Writer) {
	fmt.Fprintf(out, "Latest reads: %v/%v\n", s.NLatestReads, s.NReads)
	fmt.Fprintf(out, "Stale reads:  %v/%v\n", s.NStaleReads, s.NReads)
	fmt.Fprintf(out, "Writes:       %v\n", s.NWrites)
	fmt.Fprintf(out, "\nRead timings:\n%s\n", s.ReadTimings.Calc().String())
	fmt.Fprintf(out, "\nWrite timings:\n%s\n", s.WriteTimings.Calc().String())
}
