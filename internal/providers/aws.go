package providers

import (
	"context"
	"io"
	"math"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fatih/color"

	"github.com/dliappis/blobbench/internal/report"
)

// S3 ...
type S3 struct {
	S3Client   *s3.Client
	BufferSize uint64
	BucketName string
	FilePath   string
	FileNumber int
	Key        string

	Results *report.Results
}

// Process ...
func (p *S3) Process() error {
	color.HiMagenta("DEBUG working on file [%s]", p.FilePath)
	var metricRecord report.MetricRecord
	metricRecord.File = p.FilePath
	metricRecord.Idx = p.FileNumber

	stopwatch := time.Now()

	req := p.S3Client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(p.BucketName),
		Key:    aws.String(p.Key),
	})

	resp, err := req.Send(context.Background())

	if err != nil {
		metricRecord.FirstGet = math.MaxInt64
		metricRecord.LastGet = math.MaxInt64
		metricRecord.Size = -1
		metricRecord.Success = false
		metricRecord.ErrDetails = p.processAWSError(err)
		p.Results.Push(metricRecord)
		return err
	}

	firstGet := time.Now().Sub(stopwatch)

	// create a buffer to copy the S3 object body to
	var buf = make([]byte, p.BufferSize)
	var size int
	for {
		n, err := resp.Body.Read(buf)

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
			metricRecord.ErrDetails = p.processAWSError(err)
			p.Results.Push(metricRecord)
			return err
		}
	}

	_ = resp.Body.Close()

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

func (p *S3) processAWSError(err error) report.MetricError {
	// https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/handling-errors.html
	if err, ok := err.(awserr.Error); ok {
		return report.MetricError{Code: err.Code(), Message: err.Message()}
	}
	return report.MetricError{}
}
