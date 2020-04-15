package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dliappis/blobbench/internal"
	"github.com/dliappis/blobbench/internal/pool"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

// MetricRecord contains metric records for a specific invocation of processFile
type MetricRecord struct {
	idx      int
	size     int
	file     string
	FirstGet time.Duration
	LastGet  time.Duration
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

// Items race-condition-safe method to return Results items
func (r *Results) Items() []MetricRecord {
	r.Lock()
	defer r.Unlock()
	return r.items
}

// Push race-confition-safe method to push into Results
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
	downloadCmd.MarkFlagRequired("suffixdigits")
	downloadCmd.Flags().StringVar(&suffixseparator, "suffixsep", "-", "The separator for suffix e.g. 0 for -0000.")

	downloadCmd.Flags().IntVar(&numWorkers, "workers", 5, "Amount of parallel download workers")
	downloadCmd.Flags().Uint64Var(&bufsize, "bufsize", 1024, "Buf size to use while download (per worker)")

}

func initDownload(cmd *cobra.Command, args []string) {
	s3client := s3.New(internal.SetupS3Client(Region))
	color.Green(">>> Threadpool started")

	pool, _ := pool.NewPool(pool.Config{NumWorkers: numWorkers})
	results := &Results{}

	for i := 1; i <= numFiles; i++ {
		ctx := context.Background()

		suffix := i
		task := func() {
			// ----- TaskFunc definition -------------------------------
			err := processFile(s3client, suffix, results)
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

	color.Yellow("Results following\n")
	color.Yellow(strings.Repeat("-", 80))

	color.Green("Sample|File|TimeToFirstGet (ms)|TimeToLastGet (ms)|Size (MB)")
	sort.Sort(ByIdx(results.Items()))
	for _, v := range results.Items() {
		color.Green("%d|%s|%.1f|%.1f|%.1f|", v.idx, v.file, float64(v.FirstGet/time.Millisecond), float64(v.LastGet/time.Millisecond), float64(v.size/1024))
	}
	fmt.Println()
}

func processFile(s3client *s3.Client, suffix int, results *Results) error {
	key := fmt.Sprintf("%s/%s%s%0*d", basedir, prefix, suffixseparator, suffixdigits, suffix)
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
			// TODO see https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/handling-errors.html
			panic("Error reading object body: " + err.Error())
		}
	}

	_ = resp.Body.Close()

	lastGet := time.Now().Sub(stopwatch)

	results.Push(MetricRecord{FirstGet: firstGet, LastGet: lastGet, idx: suffix, file: fmt.Sprintf("%s", key), size: size})
	return nil
}
