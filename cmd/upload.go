package cmd

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/dliappis/blobbench/internal"

	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	"github.com/spf13/cobra"
)

var filename string
var partsize int64
var destdir string

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

	uploadCmd.Flags().StringVar(&filename, "filename", "", "The filename to upload")
	uploadCmd.MarkFlagRequired("filename")
	uploadCmd.Flags().StringVar(&destdir, "destdir", "", "The destination directory on the bucket")
	uploadCmd.MarkFlagRequired("destdir")

	uploadCmd.Flags().Int64Var(&partsize, "partsize", 5242880, "part size in bytes")
}

func upload(cmd *cobra.Command, args []string) {
	uploader := s3manager.NewUploader(internal.SetupS3Client(Region))

	f, err := os.Open(filename)
	if err != nil {
		fmt.Println(fmt.Errorf("Failed to open file %q, %v", filename, err))
	}

	// Upload the file to S3!
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(BucketName),
		Key:    aws.String(fmt.Sprintf("%s/%s", destdir, filename)),
		Body:   f,
	}, func(u *s3manager.Uploader) {
		u.PartSize = partsize
	})
	if err != nil {
		fmt.Println(fmt.Errorf("failed to upload file, %v", err))
	}

	fmt.Printf("file uploaded to [%s]\n", aws.StringValue(&result.Location))
}
