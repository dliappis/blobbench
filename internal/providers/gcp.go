package providers

import (
	"io"
	"os"
	"path/filepath"
	"time"

	"cloud.google.com/go/storage"
	"github.com/fatih/color"
	"golang.org/x/net/context"
	"google.golang.org/api/googleapi"

	"github.com/dliappis/blobbench/internal/report"
)

// GCS ...
type GCS struct {
	GCSClient     *storage.Client
	BufferSize    uint64
	BucketName    string
	FilePath      string
	FileNumber    int
	LocalDirName  string
	LocalFileName string
	Results       *report.Results
}

// Upload copies a file to a GCS Bucket.
// Path to the local file and GCS destination object are defined in p.
func (p *GCS) Upload() error {
	color.HiMagenta("DEBUG working on file [%s]", p.FilePath)

	ctx := context.Background()
	f, err := os.Open(filepath.Join(p.LocalDirName, p.LocalFileName))
	if err != nil {
		return err
	}
	defer f.Close()

	wc := p.GCSClient.Bucket(p.BucketName).Object(p.FilePath).NewWriter(ctx)
	if _, err = io.Copy(wc, f); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}
	return nil
}

// Download ...
func (p *GCS) Download() error {
	color.HiMagenta("DEBUG working on file [%s]", p.FilePath)
	m := report.MetricRecord{
		File: p.FilePath,
		Idx:  p.FileNumber,
	}

	mr := MeasuringReader{
		Metric:       m,
		BufferSize:   p.BufferSize,
		Results:      p.Results,
		ProcessError: p.processError,
		Start:        time.Now(),
	}

	ctx := context.Background()
	reader, err := p.GCSClient.Bucket(p.BucketName).Object(p.FilePath).NewReader(ctx)
	if err != nil {
		return err
	}

	_, err = mr.ReadFrom(reader)
	if err != nil {
		return err
	}

	err = reader.Close()
	if err != nil {
		return err
	}
	return nil
}

func (p *GCS) processError(err error) report.MetricError {
	if err, ok := err.(*googleapi.Error); ok {
		return report.MetricError{Code: string(err.Code), Message: err.Body}
	}
	return report.MetricError{}
}

// SetupGCSClient helper to setup the GCS client
func SetupGCSClient() *storage.Client {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		panic("Failed to create client: " + err.Error())
	}
	return client
}
