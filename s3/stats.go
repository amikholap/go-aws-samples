package s3

import (
	"sync/atomic"
)

type WorkerRuntimeStats struct {
	NReads       uint32
	NWrites      uint32
	NLatestReads uint32
	NStaleReads  uint32
	CurrentKey   string
	CurrentValue string
}

func (s *WorkerRuntimeStats) GetNReads() uint32 {
	return atomic.LoadUint32(&s.NReads)
}

func (s *WorkerRuntimeStats) IncNReads() {
	atomic.AddUint32(&s.NReads, 1)
}

func (s *WorkerRuntimeStats) GetNWrites() uint32 {
	return atomic.LoadUint32(&s.NWrites)
}

func (s *WorkerRuntimeStats) IncNWrites() {
	atomic.AddUint32(&s.NWrites, 1)
}

func (s *WorkerRuntimeStats) GetNLatestReads() uint32 {
	return atomic.LoadUint32(&s.NLatestReads)
}

func (s *WorkerRuntimeStats) IncNLatestReads() {
	atomic.AddUint32(&s.NLatestReads, 1)
}

func (s *WorkerRuntimeStats) GetNStaleReads() uint32 {
	return atomic.LoadUint32(&s.NStaleReads)
}

func (s *WorkerRuntimeStats) IncNStaleReads() {
	atomic.AddUint32(&s.NStaleReads, 1)
}
