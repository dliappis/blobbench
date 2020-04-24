package providers

import (
	"math/rand"
	"time"

	"github.com/fatih/color"

	"github.com/dliappis/blobbench/internal/report"
)

// Dummy ...
type Dummy struct {
	Results    *report.Results
	FilePath   string
	FileNumber int
}

// Process ...
func (p *Dummy) Process() error {
	color.HiMagenta("DEBUG working on file [%s]", p.FilePath)
	var metricRecord report.MetricRecord
	metricRecord.File = p.FilePath
	metricRecord.Idx = p.FileNumber

	stopwatch := time.Now()

	// wait up to 500ms
	time.Sleep(time.Millisecond * time.Duration(rand.Float32()*500))

	firstGet := time.Now().Sub(stopwatch)

	time.Sleep(time.Millisecond * time.Duration(rand.Float32()*500))

	lastGet := time.Now().Sub(stopwatch)

	p.Results.Push(report.MetricRecord{
		FirstGet: firstGet,
		LastGet:  lastGet,
		Idx:      p.FileNumber,
		File:     p.FilePath,
		Size:     rand.Intn(1024 * 1024 * 1024),
		Success:  true})
	return nil
}
