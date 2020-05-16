package providers

import (
	"io"
	"time"

	"github.com/dliappis/blobbench/internal/report"
)

// MeasuringReader ...
type MeasuringReader struct {
	Metric       report.MetricRecord
	BufferSize   uint64
	Results      *report.Results
	Start        time.Time
	ProcessError func(err error) report.MetricError
}

// ReadFrom ...
func (m *MeasuringReader) ReadFrom(r io.Reader) (int64, error) {
	var (
		buf  = make([]byte, m.BufferSize)
		size int
		n    int
	)

	for {
		n, err := r.Read(buf)

		size += n

		if err == io.EOF {
			break
		}

		// if the streaming fails, exit
		if err != nil {
			m.Metric.Duration = -1
			m.Metric.Size = size
			m.Metric.Success = false
			m.Metric.ErrDetails = m.ProcessError(err)
			m.Results.Push(m.Metric)
			return int64(n), err
		}
	}

	m.Metric.Duration = time.Since(m.Start)
	m.Metric.Size = size
	m.Metric.Success = true
	m.Results.Push(m.Metric)

	return int64(n), nil
}
