package cmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/dliappis/blobbench/internal/pool"
	"github.com/dliappis/blobbench/internal/providers"
	"github.com/dliappis/blobbench/internal/report"
)

var (
	bucketDir  string
	maxFiles   int
	numWorkers int
	bufferSize uint64

	downloadCmd = &cobra.Command{
		Use:   "download",
		Short: "Stream download objects from a Bucket",
		Long:  `TODO`,
		Run:   initDownload,
	}
)

func init() {
	rootCmd.AddCommand(downloadCmd)

	// FIXME; can I use a callback to add a slash to bucketdir?
	downloadCmd.Flags().StringVar(&bucketDir, "bucketdir", "", "The location where files are stored in the bucket.")
	downloadCmd.MarkFlagRequired("bucketdir")
	downloadCmd.Flags().IntVar(&maxFiles, "maxfiles", -1, "Limits the amount of files to download. The order is undefined. -1 is unlimited.")

	downloadCmd.Flags().IntVar(&numWorkers, "workers", 5, "Amount of parallel download workers")
	downloadCmd.Flags().Uint64Var(&bufferSize, "buffersize", 8192, "Buffer size (in bytes) that each worker will use")
}

func initDownload(cmd *cobra.Command, args []string) {
	sanitizeParams()

	startTime := time.Now()
	color.Green(">>> Threadpool started")

	pool, _ := pool.NewPool(pool.Config{NumWorkers: numWorkers})
	results := &report.Results{}

	files, err := listObjects()
	if err != nil {
		color.Red("ERROR: Unable to list files from bucket: %s, directory: %s. Error: %s.", BucketName, bucketDir, err)
		os.Exit(1)
	}

	for idx, file := range files {
		if maxFiles != -1 && idx+1 > maxFiles {
			break
		}

		ctx := context.Background()
		var err error
		var task func()
		key := file

		task = func() {
			// ----- TaskFunc definition -------------------------------
			err = processDownload(key, results)
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

func processDownload(key string, results *report.Results) error {
	switch Provider {
	case "dummy":
		p := &providers.Dummy{
			Results: results,
			Key:     key,
		}
		return p.Download()
	case "aws":
		p := &providers.S3{
			S3Client:   s3.New(providers.SetupS3Client(Region)),
			BufferSize: bufferSize,
			Results:    results,
			BucketName: BucketName,
			BucketDir:  bucketDir,
			Key:        key,
		}
		return p.Download()
	case "gcp":
		p := &providers.GCS{
			GCSClient:  providers.SetupGCSClient(),
			BufferSize: bufferSize,
			Results:    results,
			BucketName: BucketName,
			BucketDir:  bucketDir,
			Key:        key,
		}
		return p.Download()
	}
	return fmt.Errorf("Unknown provider %s", Provider)
}

func listObjects() ([]string, error) {
	switch Provider {
	case "aws":
		p := &providers.S3{
			S3Client:   s3.New(providers.SetupS3Client(Region)),
			BucketName: BucketName,
			BucketDir:  bucketDir,
		}
		return p.ListObjects(maxFiles)
	case "gcp":
		p := &providers.GCS{
			GCSClient:  providers.SetupGCSClient(),
			BucketName: BucketName,
			BucketDir:  bucketDir,
		}
		return p.ListObjects(maxFiles)
	}
	return nil, nil
}

func printResults(results *report.Results, duration time.Duration) {
	sort.Sort(report.ByDuration(results.Items()))
	if OutputFile == "" {
		printResultsStdout(results, duration)
	} else {
		printResultsFile(results, duration)
	}
}

func printResultsStdout(results *report.Results, duration time.Duration) {
	color.Yellow("\nResults following\n")
	color.Yellow(strings.Repeat("-", 90))

	color.Green(resultsHeader())
	color.Green("\nSample|File|Duration (ms)|Size (MB)|Success|Err Code|Err Message")
	sort.Sort(report.ByDuration(results.Items()))
	for idx, v := range results.Items() {
		color.Green("%d|%s|%.1f|%.1f|%t|%s|%s", idx, v.File, float64(v.Duration/time.Millisecond), float64(v.Size/1024), v.Success, v.ErrDetails.Code, v.ErrDetails.Message)
	}
	color.Green(summaryOfResults(results, duration))
	fmt.Println()
}

func printResultsFile(results *report.Results, duration time.Duration) {
	f, err := os.Create(OutputFile)
	if err != nil {
		color.Red("Unable to write to [%s], err [%s]. Printing to stdout instead.", OutputFile, err)
		printResultsStdout(results, duration)
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	_, err = fmt.Fprintf(w, resultsHeader())
	checkWriteErr(err)

	_, err = fmt.Fprintf(w, "\nSample|File|Duration (ms)|Size (MB)|Throughput (MB/s)|Throughput (Mbps)|Success|Err Code|Err Message\n")
	checkWriteErr(err)

	for idx, v := range results.Items() {
		_, err = fmt.Fprintf(w, "%d|%s|%.1f|%.1f|%.1f|%.1f|%t|%s|%s\n", idx, v.File, float64(v.Duration/time.Millisecond), float64(v.Size/1024/1024), float64(v.Size*1000/1024/1024)/float64(v.Duration/time.Millisecond), float64(v.Size*8*1000/1024/1024)/float64(v.Duration/time.Millisecond), v.Success, v.ErrDetails.Code, v.ErrDetails.Message)
		checkWriteErr(err)
	}

	_, err = fmt.Fprintf(w, summaryOfResults(results, duration))
	checkWriteErr(err)
	w.Flush()
}

func resultsHeader() string {
	return fmt.Sprintf("\nMax files: [%d], Number of workers: [%d], Buffer size: [%d]\n", maxFiles, numWorkers, bufferSize)
}

func summaryOfResults(results *report.Results, duration time.Duration) string {
	var totalBytesDownloaded uint64

	for _, v := range results.Items() {
		totalBytesDownloaded += uint64(v.Size)
	}

	totalFiles := len(results.Items())

	thoughputMBps := float64(totalBytesDownloaded) / ((float64(duration) / float64(time.Millisecond)) * float64(1000))
	sumLine := fmt.Sprintf(
		"\nTotals:\n"+
			"Execution Time (human)|Execution Time (ms)|Bytes Downloaded|GB Downloaded|Throughput (MB/s)|Throughput (Gbps)|Workers|Number of Files|BufferSize (B)\n"+
			"%s|%.1f|%d|%.1f|%.1f|%.1f|%d|%d|%d", duration, float64(duration)/float64(time.Millisecond), totalBytesDownloaded, float64(totalBytesDownloaded)/float64(1024*1024*1024), thoughputMBps, float64(thoughputMBps)*8.0/1024.0, numWorkers, totalFiles, bufferSize)

	return sumLine
}

func checkWriteErr(err error) {
	if err != nil {
		panic(err)
	}
}

func sanitizeParams() {
	if bucketDir[len(bucketDir)-1:] != "/" {
		bucketDir = bucketDir + "/"
	}
}
