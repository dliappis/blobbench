package report

import (
	"sync"
	"time"
)

// MetricRecord contains metric records for a specific invocation of processFile
type MetricRecord struct {
	Idx        int
	Size       int
	File       string
	FirstGet   time.Duration
	LastGet    time.Duration
	Success    bool
	ErrDetails MetricError
}

// MetricError contains error records for a specific invocation of processFile
type MetricError struct {
	Code    string
	Message string
}

// ByIdx implements sort.Interface based on the idx field and lets us sort MetricRecord slices
type ByIdx []MetricRecord

func (item ByIdx) Len() int           { return len(item) }
func (item ByIdx) Less(i, j int) bool { return item[i].Idx < item[j].Idx }
func (item ByIdx) Swap(i, j int)      { item[i], item[j] = item[j], item[i] }

// Results contains all metric records from executed processFile tasks
type Results struct {
	sync.Mutex
	items []MetricRecord
}

// Items returns Results items.
// It is safe to call it concurrently.
func (r *Results) Items() []MetricRecord {
	r.Lock()
	defer r.Unlock()
	return r.items
}

// Push adds an item into Results.
// It is safe to call it concurrently.
func (r *Results) Push(v MetricRecord) {
	r.Lock()
	defer r.Unlock()
	r.items = append(r.items, v)
}
