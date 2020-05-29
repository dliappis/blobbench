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
	"google.golang.org/api/iterator"

	"github.com/dliappis/blobbench/internal/report"
)

// GCS ...
type GCS struct {
	GCSClient     *storage.Client
	BufferSize    uint64
	BucketName    string
	BucketDir     string
	Key           string
	LocalDirName  string
	LocalFileName string
	Results       *report.Results
}

// Upload copies a file to a GCS Bucket.
// Path to the local file and GCS destination object are defined in p.
func (p *GCS) Upload() error {
	absFilePath := filepath.Join(p.LocalDirName, p.LocalFileName)
	color.HiMagenta("DEBUG working on file [%s]", absFilePath)

	ctx := context.Background()
	f, err := os.Open(filepath.Join(p.LocalDirName, p.LocalFileName))
	if err != nil {
		return err
	}
	defer f.Close()

	wc := p.GCSClient.Bucket(p.BucketName).Object(p.Key).NewWriter(ctx)
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
	color.HiMagenta("DEBUG working on file [%s]", p.Key)
	m := report.MetricRecord{
		File: p.Key,
	}

	mr := MeasuringReader{
		Metric:       m,
		BufferSize:   p.BufferSize,
		Results:      p.Results,
		ProcessError: p.processError,
		Start:        time.Now(),
	}

	ctx := context.Background()
	reader, err := p.GCSClient.Bucket(p.BucketName).Object(p.Key).NewReader(ctx)
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

// ListObjects returns all or the first numFiles objects of a bucket under a specified prefix
func (p *GCS) ListObjects(maxFiles int) ([]string, error) {
	var files []string

	ctx := context.Background()

	ctx, cancel := context.WithTimeout(ctx, time.Second*60)
	defer cancel()
	it := p.GCSClient.Bucket(p.BucketName).Objects(ctx, &storage.Query{
		Prefix:    p.BucketDir,
		Delimiter: "/",
	})

	for {
		if maxFiles != -1 && len(files)+1 > maxFiles {
			return files, nil
		}

		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		files = append(files, attrs.Name)
	}

	return files, nil
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
