package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dliappis/blobbench/internal"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

type metricRecord struct {
	index    int
	size     int
	file     string
	FirstGet time.Duration
	LastGet  time.Duration
}

const bufferSize uint64 = 1024 * 8 // 8 kilobytes

var (
	basedir                string
	prefix                 string
	suffixseparator        string
	numFiles, suffixdigits int
	numWorkers             int
	bufsize                uint64

	wg    sync.WaitGroup
	queue = make(chan taskStruct, numWorkers)

	downloadCmd = &cobra.Command{
		Use:   "download",
		Short: "Download ...",
		Long:  `TODO`,
		Run:   initDownload,
	}
)

type worker struct {
	id int
	ch <-chan taskStruct
	wg *sync.WaitGroup
}

type taskStruct struct {
	workerFunc func(int, *s3.Client, chan<- metricRecord)
	filesuffix int
	results    chan<- metricRecord
	s3client   *s3.Client
}

func (w *worker) run() {
	go func() {
		defer w.wg.Done()
		for tStruct := range w.ch {
			// call the work function here
			color.Yellow("About to start %d", tStruct.filesuffix)
			tStruct.workerFunc(tStruct.filesuffix, tStruct.s3client, tStruct.results)
		}
	}()
}

func add(ctx context.Context, task taskStruct) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case queue <- task:
	}
	return nil
}

func wait() error {
	close(queue)
	wg.Wait()
	return nil
}

func init() {
	rootCmd.AddCommand(downloadCmd)

	downloadCmd.Flags().StringVar(&basedir, "basedir", "", "The basedir")
	downloadCmd.MarkFlagRequired("basedir")
	downloadCmd.Flags().StringVar(&prefix, "prefix", "", "The prefix of the filename")
	downloadCmd.MarkFlagRequired("prefix")
	downloadCmd.Flags().IntVar(&numFiles, "numfiles", 2, "How many files")
	downloadCmd.MarkFlagRequired("numfiles")
	downloadCmd.Flags().IntVar(&suffixdigits, "suffixdigits", 4, "suffix digits, should start from 0, e.g. 4 for -0000")
	downloadCmd.MarkFlagRequired("suffixdigits")
	downloadCmd.Flags().StringVar(&suffixseparator, "suffixsep", "-", "The separator for suffix e.g. 0 for -0000.")

	downloadCmd.Flags().IntVar(&numWorkers, "workers", 5, "Amount of parallel download workers")
	downloadCmd.Flags().Uint64Var(&bufsize, "bufsize", 1024, "Buf size to use while download (per worker)")

}

func initDownload(cmd *cobra.Command, args []string) {
	color.Green("Initializing pool with workers")
	for i := 1; i <= numWorkers; i++ {
		w := worker{id: i, ch: queue, wg: &wg}
		w.run()
	}
	wg.Add(numWorkers)

	// maybe move this up
	s3client := s3.New(internal.SetupS3Client(Region))

	results := make(chan metricRecord, numWorkers*numFiles)

	for i := 1; i <= numFiles; i++ {
		ctx := context.Background()

		tStruct := taskStruct{filesuffix: i, results: results, s3client: s3client, workerFunc: downloadTask}
		if err := add(ctx, tStruct); err != nil {
			color.Red("ERROR: Adding item: %s", err)
			os.Exit(1)
		}
	}

	if err := wait(); err != nil {
		color.Red("ERROR: Close: %s", err)
	}

	color.Green(">>> Threadpool exited")
	close(results)

	color.Yellow("Results following\n")
	color.Green("Sample|File|TimeToFirstGet|TimeToLastGet|Size")
	for res := range results {
		color.Green("%d|%s|%s|%s|%d|", res.index, res.file, res.FirstGet, res.LastGet, res.size)
	}

}

func downloadTask(filesuffix int, s3client *s3.Client, results chan<- metricRecord) {
	key := fmt.Sprintf("%s/%s%s%0*d", basedir, prefix, suffixseparator, suffixdigits, filesuffix)
	color.HiMagenta("DEBUG working on file [%s/%s]", BucketName, key)
	stopwatch := time.Now()
	req := s3client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(BucketName),
		Key:    aws.String(key),
	})

	resp, err := req.Send(context.Background())

	if err != nil {
		panic("Failed to get object: " + err.Error())
	}

	firstGet := time.Now().Sub(stopwatch)

	// create a buffer to copy the S3 object body to
	// TODO specify somewhere the buffer size, for now it's 1MB
	var buf = make([]byte, bufferSize)

	// read the s3 object body into the buffer
	size := 0
	for {
		n, err := resp.Body.Read(buf)

		size += n

		if err == io.EOF {
			break
		}

		// if the streaming fails, exit
		if err != nil {
			// TODO see https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/handling-errors.html
			panic("Error reading object body: " + err.Error())
		}
	}

	_ = resp.Body.Close()

	lastGet := time.Now().Sub(stopwatch)

	// send measurements
	results <- metricRecord{FirstGet: firstGet, LastGet: lastGet, index: filesuffix, file: fmt.Sprintf("%s", key), size: size}
}
