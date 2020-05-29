package providers

import (
	"fmt"
	"io"
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
	Key           string
	LocalDirName  string
	LocalFileName string
}

// SleepingReader ...
type SleepingReader struct{}

func (r *SleepingReader) Read(p []byte) (int, error) {
	// wait up to 500ms
	time.Sleep(time.Millisecond * time.Duration(rand.Float32()*500))
	return 0, io.EOF
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
	color.HiMagenta("DEBUG working on file [%s]", p.Key)
	var err error
	m := report.MetricRecord{
		File: p.Key,
	}

	mr := MeasuringReader{
		Metric:       m,
		BufferSize:   0,
		Results:      p.Results,
		ProcessError: p.processError,
		Start:        time.Now(),
	}

	_, err = mr.ReadFrom(&SleepingReader{})
	if err != nil {
		return err
	}

	return nil
}

func (p *Dummy) processError(err error) report.MetricError {
	if err != nil {
		rand.Seed(time.Now().UnixNano())
		return report.MetricError{Code: string(rand.Intn(500) + 1), Message: "Dummy provider error"}
	}
	return report.MetricError{}
}
