package cmd

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/aws/endpoints"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	"github.com/spf13/cobra"
)

// Filename TODO
var Filename string

// Partsize TODO
var Partsize int64

// DestDir TODO
var DestDir string

var (
	uploadCmd = &cobra.Command{
		Use:   "upload",
		Short: "Upload multipart",
		Long:  `TODO`,
		Run:   upload,
	}
)

func init() {
	rootCmd.AddCommand(uploadCmd)

	uploadCmd.Flags().StringVar(&Filename, "filename", "", "The filename to upload")
	uploadCmd.MarkFlagRequired("filename")
	uploadCmd.Flags().StringVar(&DestDir, "destdir", "", "The destination directory on the bucket")
	uploadCmd.MarkFlagRequired("destdir")

	uploadCmd.Flags().Int64Var(&Partsize, "partsize", 5242880, "part size in bytes")
}

func upload(cmd *cobra.Command, args []string) {
	setupS3Client()
}

func setupS3Client() {
	// gets the AWS credentials from the default file or from the EC2 instance profile
	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		panic("Unable to load AWS SDK config: " + err.Error())
	}

	// set the SDK region to either the one from the program arguments or else to the same region as the EC2 instance
	cfg.Region = endpoints.UsEast2RegionID

	// set a 3-minute timeout for all S3 calls, including downloading the body
	cfg.HTTPClient = &http.Client{
		Timeout: time.Second * 180,
	}

	uploader := s3manager.NewUploader(cfg)

	f, err := os.Open(Filename)
	if err != nil {
		fmt.Println(fmt.Errorf("Failed to open file %q, %v", Filename, err))
	}

	// Upload the file to S3!
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(BucketName),
		Key:    aws.String(fmt.Sprintf("%s/%s", DestDir, Filename)),
		Body:   f,
	}, func(u *s3manager.Uploader) {
		u.PartSize = Partsize
	})
	if err != nil {
		fmt.Println(fmt.Errorf("failed to upload file, %v", err))
	}

	fmt.Printf("file uploaded to [%s]\n", aws.StringValue(&result.Location))
}
