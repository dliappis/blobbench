package providers

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/fatih/color"

	"github.com/dliappis/blobbench/internal/report"
)

// Dummy ...
type Dummy struct {
	Results       *report.Results
	FilePath      string
	LocalDirName  string
	LocalFileName string
	FileNumber    int
}

// Upload simulates upload of a file to a Blob store.
// Local path is defined in p.
func (p *Dummy) Upload() error {
	absFilePath := filepath.Join(p.LocalDirName, p.LocalFileName)
	color.HiMagenta("DEBUG working on file [%s]", absFilePath)

	_, err := os.Open(absFilePath)
	if err != nil {
		fmt.Println(fmt.Errorf("Failed to open file %q, %v", absFilePath, err))
	}

	// wait up to 500ms
	time.Sleep(time.Millisecond * time.Duration(rand.Float32()*500))
	return nil
}

// Download ...
func (p *Dummy) Download() error {
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
