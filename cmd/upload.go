package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dliappis/blobbench/internal/pool"
	"github.com/dliappis/blobbench/internal/providers"
	"github.com/dliappis/blobbench/internal/report"
	"github.com/fatih/color"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/cobra"
)

var localdirname string
var partsize int64
var destdir string

var (
	uploadCmd = &cobra.Command{
		Use:   "upload",
		Short: "Multi Vendor Uploader",
		Long:  `TODO`,
		Run:   initUpload,
	}
)

func init() {
	rootCmd.AddCommand(uploadCmd)

	uploadCmd.Flags().StringVar(&localdirname, "localdir", "", "The local directory where files to be uploaded are located")
	uploadCmd.MarkFlagRequired("localdir")

	uploadCmd.Flags().IntVar(&numWorkers, "workers", 5, "Amount of parallel upload workers")
	uploadCmd.Flags().Uint64Var(&bufferSize, "bufsize", 8192, "Buffer size (in bytes) that each worker will use")

	uploadCmd.Flags().StringVar(&destdir, "destdir", "", "The destination directory on the bucket")
	uploadCmd.MarkFlagRequired("destdir")

	uploadCmd.Flags().Int64Var(&partsize, "partsize", 5242880, "part size in bytes for multipartuploads")
}

func localFileNames() []string {
	localdir, err := os.Open(localdirname)
	if err != nil {
		panic(fmt.Sprintf("unable to find directory %s locally", localdirname))
	}

	localfiles, err := localdir.Readdirnames(0)
	if err != nil {
		// TODO log instead
		panic(fmt.Sprintf("Hit error %s while reading the contents of the directory %s", err, localdirname))
	}
	return localfiles
}

func absDirPath(filename string) string {
	f, err := os.Stat(filename)
	if err != nil {
		// TODO return err and exit cobra with an err
		panic(fmt.Errorf("Error accessing Parameter [%s]", filename))
	}

	if f.IsDir() {
		dirName, _ := filepath.Abs(filename)
		return dirName
	}
	// TODO return err and exit cobra with an err
	panic(fmt.Errorf("Parameter [%s] is not a valid directory", filename))
}

func initUpload(cmd *cobra.Command, args []string) {
	startTime := time.Now()
	color.Green(">>> Threadpool started")

	absDir := absDirPath(localdirname)

	pool, _ := pool.NewPool(pool.Config{NumWorkers: numWorkers})
	results := &report.Results{}

	for i, localFileName := range localFileNames() {
		ctx := context.Background()
		var err error
		var task func()
		dirName := absDir
		fileName := localFileName
		fileNo := i
		task = func() {
			// ----- TaskFunc definition -------------------------------
			err = processUpload(fileNo, dirName, fileName, results)
			// ---------------------------------------------------------

			if err != nil {
				color.Red("ERROR: ", err)
			}
		}

		if err := pool.Add(ctx, task); err != nil {
			color.Red("ERROR: Adding item: %s", err)
			os.Exit(1)
		}
	}

	if err := pool.Wait(); err != nil {
		color.Red("ERROR: Closing: %s", err)
	}

	color.Green(">>> Threadpool exited\n\n")

	duration := time.Since(startTime)
	printResults(results, duration)
}

func processUpload(fileNo int, dirName string, fileName string, results *report.Results) error {
	path := fmt.Sprintf("%s/%s", destdir, fileName)

	switch Provider {
	case "dummy":
		p := &providers.Dummy{
			Results:       results,
			FilePath:      path,
			FileNumber:    fileNo,
			LocalDirName:  dirName,
			LocalFileName: fileName,
		}
		return p.Upload()
	case "aws":
		p := &providers.S3{
			S3Client:      s3.New(providers.SetupS3Client(Region)),
			BufferSize:    bufferSize,
			Results:       results,
			FilePath:      path,
			FileNumber:    fileNo,
			BucketName:    BucketName,
			LocalDirName:  dirName,
			LocalFileName: fileName,
			PartSize:      partsize,
		}
		return p.Upload()
	case "gcp":
		p := &providers.GCS{
			GCSClient:     providers.SetupGCSClient(),
			BufferSize:    bufferSize,
			Results:       results,
			FilePath:      path,
			FileNumber:    fileNo,
			BucketName:    BucketName,
			LocalDirName:  dirName,
			LocalFileName: fileName,
		}
		return p.Upload()
	}
	return fmt.Errorf("Unknown provider %s", Provider)
}
