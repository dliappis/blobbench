package providers

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/dliappis/blobbench/internal/report"
	"github.com/fatih/color"
	"golang.org/x/net/context"
	"google.golang.org/api/googleapi"
)

// AZBlob ...
type AZBlob struct {
	ServiceURL    azblob.ServiceURL
	BufferSize    uint64
	BucketName    string
	BucketDir     string
	Key           string
	LocalDirName  string
	LocalFileName string
	Results       *report.Results
}

// Upload copies a file to an Azure Container (Bucket).
// Path to the local file and Azure destination blob are defined in p.
func (p *AZBlob) Upload() error {
	absFilePath := filepath.Join(p.LocalDirName, p.LocalFileName)
	color.HiMagenta("DEBUG working on file [%s]", absFilePath)

	ctx := context.Background()
	f, err := os.Open(filepath.Join(p.LocalDirName, p.LocalFileName))
	if err != nil {
		return err
	}
	defer f.Close()

	containerURL := p.ServiceURL.NewContainerURL(p.BucketName)
	blobURL := containerURL.NewBlockBlobURL(p.Key)
	uploadToBlockBlobOptions := azblob.UploadToBlockBlobOptions{
		BlockSize: 2 << 16, // 64MB
		BlobHTTPHeaders: azblob.BlobHTTPHeaders{
			ContentType:        "application/octet-stream",
			ContentDisposition: "attachment",
		},
		Metadata:         azblob.Metadata{},
		AccessConditions: azblob.BlobAccessConditions{},
	}

	_, err = azblob.UploadFileToBlockBlob(ctx, f, blobURL, uploadToBlockBlobOptions)
	if err != nil {
		return err
	}
	return nil
}

// Download reads a blob from a container (bucket).
func (p *AZBlob) Download() error {
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
	containerURL := p.ServiceURL.NewContainerURL(p.BucketName)
	blobURL := containerURL.NewBlockBlobURL(p.Key)
	get, err := blobURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return err
	}

	reader := get.Body(azblob.RetryReaderOptions{})
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

func (p *AZBlob) processError(err error) report.MetricError {
	if err, ok := err.(*googleapi.Error); ok {
		return report.MetricError{Code: string(err.Code), Message: err.Body}
	}
	return report.MetricError{}
}

// ListObjects returns all or the first numFiles objects of a bucket under a specified prefix
func (p *AZBlob) ListObjects(maxFiles int) ([]string, error) {
	var files []string

	ctx := context.Background()

	ctx, cancel := context.WithTimeout(ctx, time.Second*60)
	defer cancel()

	containerURL := p.ServiceURL.NewContainerURL(p.BucketName)

	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerURL.ListBlobsHierarchySegment(ctx, marker, "/", azblob.ListBlobsSegmentOptions{
			Prefix: p.BucketDir,
		})
		if err != nil {
			return nil, err
		}
		// IMPORTANT: ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			if maxFiles != -1 && len(files)+1 > maxFiles {
				return files, nil
			}
			files = append(files, blobInfo.Name)
		}
	}

	return files, nil
}

// SetupServiceURL helper to setup the Azure request pipeline
func SetupServiceURL(bufferSize uint64, accountName string, accountKey string) azblob.ServiceURL {
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		panic(fmt.Sprintf("Unable to create Azure client with provided credentials. Error %s", err))
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))

	return azblob.NewServiceURL(*u, p)
}
