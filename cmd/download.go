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

	"github.com/dliappis/blobbench/internal"
	"github.com/dliappis/blobbench/internal/pool"
	"github.com/dliappis/blobbench/internal/providers"
	"github.com/dliappis/blobbench/internal/report"
)

var bufferSize uint64

var (
	basedir                string
	prefix                 string
	suffixseparator        string
	numFiles, suffixdigits int
	numWorkers             int
	bufsize                uint64

	downloadCmd = &cobra.Command{
		Use:   "download",
		Short: "Download ...",
		Long:  `TODO`,
		Run:   initDownload,
	}
)

func init() {
	rootCmd.AddCommand(downloadCmd)

	downloadCmd.Flags().StringVar(&basedir, "basedir", "", "The basedir")
	downloadCmd.MarkFlagRequired("basedir")
	downloadCmd.Flags().StringVar(&prefix, "prefix", "", "The prefix of the filename")
	downloadCmd.MarkFlagRequired("prefix")
	downloadCmd.Flags().IntVar(&numFiles, "numfiles", 2, "How many files")
	downloadCmd.MarkFlagRequired("numfiles")
	downloadCmd.Flags().IntVar(&suffixdigits, "suffixdigits", 4, "suffix digits, should start from 0, e.g. 4 for -0000")
	downloadCmd.Flags().StringVar(&suffixseparator, "suffixsep", "-", "The separator for suffix e.g. 0 for -0000")

	downloadCmd.Flags().IntVar(&numWorkers, "workers", 5, "Amount of parallel download workers")
	downloadCmd.Flags().Uint64Var(&bufferSize, "bufsize", 8192, "Buffer size (in bytes) that each worker will use")
}

func initDownload(cmd *cobra.Command, args []string) {
	startTime := time.Now()
	color.Green(">>> Threadpool started")

	pool, _ := pool.NewPool(pool.Config{NumWorkers: numWorkers})
	results := &report.Results{}

	for i := 0; i < numFiles; i++ {
		ctx := context.Background()
		var err error
		var task func()
		suffix := i

		task = func() {
			// ----- TaskFunc definition -------------------------------
			err = processDownload(suffix, results)
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

func processDownload(suffix int, results *report.Results) error {
	path := fmt.Sprintf("%s/%s%s%0*d", basedir, prefix, suffixseparator, suffixdigits, suffix)

	switch Provider {
	case "dummy":
		p := &providers.Dummy{
			Results:    results,
			FilePath:   path,
			FileNumber: suffix,
		}
		return p.Download()
	case "aws":
		key := fmt.Sprintf("%s/%s%s%0*d", basedir, prefix, suffixseparator, suffixdigits, suffix)
		p := &providers.S3{
			S3Client:   s3.New(internal.SetupS3Client(Region)),
			BufferSize: bufferSize,
			Results:    results,
			FilePath:   path,
			FileNumber: suffix,
			BucketName: BucketName,
			Key:        key,
		}
		return p.Download()
	case "gcp":
		key := fmt.Sprintf("%s/%s%s%0*d", basedir, prefix, suffixseparator, suffixdigits, suffix)
		p := &providers.GCS{
			GCSClient:  internal.SetupGCSClient(),
			BufferSize: bufferSize,
			Results:    results,
			FilePath:   path,
			FileNumber: suffix,
			BucketName: BucketName,
			Key:        key,
		}
		return p.Download()
	}
	return fmt.Errorf("Unknown provider %s", Provider)
}

func printResults(results *report.Results, duration time.Duration) {
	sort.Sort(report.ByIdx(results.Items()))
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
	sort.Sort(report.ByIdx(results.Items()))
	for _, v := range results.Items() {
		color.Green("%d|%s|%.1f|%.1f|%t|%s|%s", v.Idx, v.File, float64(v.Duration/time.Millisecond), float64(v.Size/1024), v.Success, v.ErrDetails.Code, v.ErrDetails.Message)
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

	for _, v := range results.Items() {
		_, err = fmt.Fprintf(w, "%d|%s|%.1f|%.1f|%.1f|%.1f|%t|%s|%s\n", v.Idx, v.File, float64(v.Duration/time.Millisecond), float64(v.Size/1024/1024), float64(v.Size*1000/1024/1024)/float64(v.Duration/time.Millisecond), float64(v.Size*8*1000/1024/1024)/float64(v.Duration/time.Millisecond), v.Success, v.ErrDetails.Code, v.ErrDetails.Message)
		checkWriteErr(err)
	}

	_, err = fmt.Fprintf(w, summaryOfResults(results, duration))
	checkWriteErr(err)
	w.Flush()
}

func resultsHeader() string {
	return fmt.Sprintf("\nNumber of files: [%d], Number of workers: [%d], Buffer size: [%d]\n", numFiles, numWorkers, bufferSize)
}

func summaryOfResults(results *report.Results, duration time.Duration) string {
	var totalBytesDownloaded uint64
	for _, v := range results.Items() {
		totalBytesDownloaded += uint64(v.Size)
	}

	thoughputMBps := float64(totalBytesDownloaded) / ((float64(duration) / float64(time.Millisecond)) * float64(1000))
	sumLine := fmt.Sprintf(
		"\nTotals:\n"+
			"Execution Time (human)|Execution Time (ms)|Bytes Downloaded|GB Downloaded|Throughput (MB/s)|Throughput (Gbps)|Workers|Number of Files|BufferSize (B)\n"+
			"%s|%.1f|%d|%.1f|%.1f|%.1f|%d|%d|%d", duration, float64(duration)/float64(time.Millisecond), totalBytesDownloaded, float64(totalBytesDownloaded)/float64(1024*1024*1024), thoughputMBps, float64(thoughputMBps)*8.0/1024.0, numWorkers, numFiles, bufferSize)

	return sumLine
}

func checkWriteErr(err error) {
	if err != nil {
		panic(err)
	}
}
