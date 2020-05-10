package providers

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
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
	// Used only for uploads
	LocalDirName  string
	LocalFileName string
	PartSize      int64
	Results       *report.Results
}

// Upload copies a file to an S3 Bucket.
// Path to the local file and S3 destination object are defined in p.
func (p *S3) Upload() error {
	color.HiMagenta("DEBUG working on file [%s]", p.FilePath)
	uploader := s3manager.NewUploader(p.S3Client.Config)

	f, err := os.Open(filepath.Join(p.LocalDirName, p.LocalFileName))
	if err != nil {
		fmt.Println(fmt.Errorf("Failed to open file %q, %v", p.LocalFileName, err))
	}
	defer f.Close()

	// Upload the file to S3!
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(p.BucketName),
		Key:    aws.String(p.Key),
		Body:   f,
	}, func(u *s3manager.Uploader) {
		u.PartSize = p.PartSize
	})
	if err != nil {
		fmt.Println(fmt.Errorf("failed to upload file, %v", err))
	}

	fmt.Printf("file uploaded to [%s]\n", aws.StringValue(&result.Location))
	return nil
}

// Download ...
func (p *S3) Download() error {
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
