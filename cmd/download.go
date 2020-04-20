package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dliappis/blobbench/internal"
	"github.com/dliappis/blobbench/internal/pool"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

// MetricRecord contains metric records for a specific invocation of processFile
type MetricRecord struct {
	idx        int
	size       int
	file       string
	FirstGet   time.Duration
	LastGet    time.Duration
	success    bool
	errDetails metricError
}

// metricError contains error records for a specific invocation of processFile
type metricError struct {
	code    string
	message string
}

// ByIdx implements sort.Interface based on the idx field and lets us sort MetricRecord slices
type ByIdx []MetricRecord

func (item ByIdx) Len() int           { return len(item) }
func (item ByIdx) Less(i, j int) bool { return item[i].idx < item[j].idx }
func (item ByIdx) Swap(i, j int)      { item[i], item[j] = item[j], item[i] }

// Results contains all metric records from executed processFile tasks
type Results struct {
	sync.Mutex
	items []MetricRecord
}

// Items returns Results items.
// It is safe to call it concurrently.
func (r *Results) Items() []MetricRecord {
	r.Lock()
	defer r.Unlock()
	return r.items
}

// Push adds an item into Results.
// It is safe to call it concurrently.
func (r *Results) Push(v MetricRecord) {
	r.Lock()
	defer r.Unlock()
	r.items = append(r.items, v)
}

const bufferSize uint64 = 1024 * 8 // 8 kilobytes

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
	downloadCmd.Flags().StringVar(&suffixseparator, "suffixsep", "-", "The separator for suffix e.g. 0 for -0000.")

	downloadCmd.Flags().IntVar(&numWorkers, "workers", 5, "Amount of parallel download workers")
	downloadCmd.Flags().Uint64Var(&bufsize, "bufsize", 1024, "Buf size to use while download (per worker)")
}

func initDownload(cmd *cobra.Command, args []string) {
	startTime := time.Now()
	s3client := s3.New(internal.SetupS3Client(Region))
	color.Green(">>> Threadpool started")

	pool, _ := pool.NewPool(pool.Config{NumWorkers: numWorkers})
	results := &Results{}

	for i := 0; i < numFiles; i++ {
		ctx := context.Background()
		var err error
		var task func()
		suffix := i

		task = func() {
			// ----- TaskFunc definition -------------------------------
			err = processFile(s3client, suffix, results)
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

func processFile(s3client *s3.Client, suffix int, results *Results) error {
	if DryRun {
		return processFileDryRun(suffix, results)
	} else {
		return processFileAWS(s3client, suffix, results)
	}
}

func processFileAWS(s3client *s3.Client, suffix int, results *Results) error {
	key := fmt.Sprintf("%s/%s%s%0*d", basedir, prefix, suffixseparator, suffixdigits, suffix)
	color.HiMagenta("DEBUG working on file [%s/%s]", BucketName, key)
	var metricRecord MetricRecord
	metricRecord.file = key
	metricRecord.idx = suffix

	stopwatch := time.Now()

	req := s3client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(BucketName),
		Key:    aws.String(key),
	})

	resp, err := req.Send(context.Background())

	if err != nil {
		metricRecord.FirstGet = math.MaxInt64
		metricRecord.LastGet = math.MaxInt64
		metricRecord.size = -1
		metricRecord.success = false
		metricRecord.errDetails = processAWSError(err)
		results.Push(metricRecord)
		return err
	}

	firstGet := time.Now().Sub(stopwatch)

	// create a buffer to copy the S3 object body to
	var buf = make([]byte, bufferSize)
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
			metricRecord.size = size
			metricRecord.success = false
			metricRecord.errDetails = processAWSError(err)
			results.Push(metricRecord)
			return err
		}
	}

	_ = resp.Body.Close()

	lastGet := time.Now().Sub(stopwatch)

	results.Push(MetricRecord{FirstGet: firstGet, LastGet: lastGet, idx: suffix, file: fmt.Sprintf("%s", key), size: size, success: true})
	return nil
}

func processFileDryRun(suffix int, results *Results) error {
	key := fmt.Sprintf("%s/%s%s%0*d", basedir, prefix, suffixseparator, suffixdigits, suffix)
	color.HiMagenta("DEBUG working on file [%s/%s]", BucketName, key)
	var metricRecord MetricRecord
	metricRecord.file = key
	metricRecord.idx = suffix

	stopwatch := time.Now()

	// wait up to 500ms
	time.Sleep(time.Millisecond * time.Duration(rand.Float32()*500))

	firstGet := time.Now().Sub(stopwatch)

	time.Sleep(time.Millisecond * time.Duration(rand.Float32()*500))

	lastGet := time.Now().Sub(stopwatch)

	results.Push(MetricRecord{FirstGet: firstGet, LastGet: lastGet, idx: suffix, file: fmt.Sprintf("%s", key), size: rand.Intn(1024 * 1024 * 1024), success: true})
	return nil
}

func processAWSError(err error) metricError {
	// https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/handling-errors.html
	if aerr, ok := err.(awserr.Error); ok {
		return metricError{code: aerr.Code(), message: aerr.Message()}
	}
	return metricError{}
}

func printResults(results *Results, duration time.Duration) {
	sort.Sort(ByIdx(results.Items()))
	if OutputFile == "" {
		printResultsStdout(results, duration)
	} else {
		printResultsFile(results, duration)
	}
}

func printResultsStdout(results *Results, duration time.Duration) {
	color.Yellow("\nResults following\n")
	color.Yellow(strings.Repeat("-", 90))

	color.Green("Sample|File|TimeToFirstGet (ms)|TimeToLastGet (ms)|Size (MB)|Success|Err Code|Err Message")
	sort.Sort(ByIdx(results.Items()))
	for _, v := range results.Items() {
		color.Green("%d|%s|%.1f|%.1f|%.1f|%t|%s|%s", v.idx, v.file, float64(v.FirstGet/time.Millisecond), float64(v.LastGet/time.Millisecond), float64(v.size/1024), v.success, v.errDetails.code, v.errDetails.message)
	}
	color.Green("\nTotal execution time: %s", duration)
	fmt.Println()
}

func printResultsFile(results *Results, duration time.Duration) {
	f, err := os.Create(OutputFile)
	if err != nil {
		color.Red("Unable to write to [%s], err [%s]. Printing to stdout instead.", OutputFile, err)
		printResultsStdout(results, duration)
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	_, err = fmt.Fprintf(w, "Sample|File|TimeToFirstGet (ms)|TimeToLastGet (ms)|Size (MB)|Success|Err Code|Err Message\n")
	checkWriteErr(err)

	for _, v := range results.Items() {
		_, err = fmt.Fprintf(w, "%d|%s|%.1f|%.1f|%.1f|%t|%s|%s\n", v.idx, v.file, float64(v.FirstGet/time.Millisecond), float64(v.LastGet/time.Millisecond), float64(v.size/1024), v.success, v.errDetails.code, v.errDetails.message)
		checkWriteErr(err)
	}

	_, err = fmt.Fprintf(w, "\nTotal execution time: %s\n", duration)
	checkWriteErr(err)
	w.Flush()
}

func checkWriteErr(err error) {
	if err != nil {
		panic(err)
	}
}
