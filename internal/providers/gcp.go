package providers

import (
	"io"
	"math"
	"time"

	"cloud.google.com/go/storage"
	"github.com/fatih/color"
	"golang.org/x/net/context"
	"google.golang.org/api/googleapi"

	"github.com/dliappis/blobbench/internal/report"
)

// GCS ...
type GCS struct {
	GCSClient  *storage.Client
	BufferSize uint64
	BucketName string
	FilePath   string
	FileNumber int
	Key        string

	Results *report.Results
}

// Process ...
func (p *GCS) Process() error {
	color.HiMagenta("DEBUG working on file [%s]", p.FilePath)
	var metricRecord report.MetricRecord
	metricRecord.File = p.FilePath
	metricRecord.Idx = p.FileNumber

	ctx := context.Background()
	stopwatch := time.Now()
	reader, err := p.GCSClient.Bucket(p.BucketName).Object(p.Key).NewReader(ctx)

	if err != nil {
		metricRecord.FirstGet = math.MaxInt64
		metricRecord.LastGet = math.MaxInt64
		metricRecord.Size = -1
		metricRecord.Success = false
		metricRecord.ErrDetails = p.processGCSError(err)
		p.Results.Push(metricRecord)
		return err
	}

	// TODO rename, the term firstGET is S3 specific
	firstGet := time.Now().Sub(stopwatch)

	// create a buffer to copy the GCS object body to
	var buf = make([]byte, p.BufferSize)
	var size int

	for {
		// TODO consider is we should instead read with io.Copy as shown in https://godoc.org/cloud.google.com/go/storage
		n, err := reader.Read(buf)
		size += n

		if err == io.EOF {
			break
		}

		// if the streaming fails, exit
		if err != nil {
			metricRecord.FirstGet = firstGet
			metricRecord.LastGet = -1
			metricRecord.Size = size
			metricRecord.Success = false
			metricRecord.ErrDetails = p.processGCSError(err)
			p.Results.Push(metricRecord)
			return err
		}
	}

	_ = reader.Close()

	lastGet := time.Now().Sub(stopwatch)

	p.Results.Push(report.MetricRecord{
		FirstGet: firstGet,
		LastGet:  lastGet,
		Idx:      p.FileNumber,
		File:     p.Key,
		Size:     size,
		Success:  true,
	})
	return nil
}

func (p *GCS) processGCSError(err error) report.MetricError {
	// https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/handling-errors.html
	if err, ok := err.(*googleapi.Error); ok {
		return report.MetricError{Code: string(err.Code), Message: err.Body}
	}
	return report.MetricError{}
}
