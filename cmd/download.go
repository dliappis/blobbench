package cmd

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dliappis/blobbench/internal"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

type serviceTime struct {
	index    int
	file     string
	FirstGet time.Duration
	LastGet  time.Duration
}

var bufferSize uint64 = 1024 * 8 // 8 kilobytes

var (
	basedir                string
	prefix                 string
	suffixseparator        string
	numfiles, suffixdigits int
	numtasks               int
	bufsize                uint64

	downloadCmd = &cobra.Command{
		Use:   "download",
		Short: "Download ...",
		Long:  `TODO`,
		Run:   download,
	}
)

func init() {
	rootCmd.AddCommand(downloadCmd)

	downloadCmd.Flags().StringVar(&basedir, "basedir", "", "The basedir")
	downloadCmd.MarkFlagRequired("basedir")
	downloadCmd.Flags().StringVar(&prefix, "prefix", "", "The prefix of the filename")
	downloadCmd.MarkFlagRequired("prefix")
	downloadCmd.Flags().IntVar(&numfiles, "numfiles", 2, "How many files")
	downloadCmd.MarkFlagRequired("numfiles")
	downloadCmd.Flags().IntVar(&suffixdigits, "suffixdigits", 4, "suffix digits, should start from 0, e.g. 4 for -0000")
	downloadCmd.MarkFlagRequired("suffixdigits")
	downloadCmd.Flags().StringVar(&suffixseparator, "suffixsep", "-", "The separator for suffix e.g. 0 for -0000.")

	downloadCmd.Flags().IntVar(&numtasks, "tasks", 5, "Amount of parallel download workers")
	downloadCmd.Flags().Uint64Var(&bufsize, "bufsize", 1024, "Buf size to use while download (per worker)")

}

func download(cmd *cobra.Command, args []string) {
	color.Green(">>> Starting download benchmarks")
	var allresults []serviceTime
	results := make(chan serviceTime, numfiles)
	s3client := s3.New(internal.SetupS3Client(Region))
	taskGroupChunks := numfiles / numtasks

	for taskGroupIdx := 1; taskGroupIdx <= taskGroupChunks+1; taskGroupIdx++ {
		tasks := numtasks
		if taskGroupIdx == taskGroupChunks+1 {
			// last batch may include less tasks that numtasks
			tasks = numfiles % numtasks
		}

		tasksPool := make(chan int, tasks)
		for curChan := 1; curChan <= tasks; curChan++ {
			go downloadTask(taskGroupIdx, s3client, tasksPool, results)
		}

		for j := 1; j <= tasks; j++ {
			color.Red("Spawning task %d", j)
			tasksPool <- ((taskGroupIdx - 1) * numtasks) + (j - 1)
		}
		close(tasksPool)
		// collect results
		for s := 0; s < tasks; s++ {
			allresults = append(allresults, <-results)
		}

	}
	// print results
	for _, ss := range allresults {
		color.Green("Sample %d, Suffix: %s, FirstGet %v, LastGet %v", ss.index, ss.file, ss.FirstGet, ss.LastGet)
	}

}

func downloadTask(taskGroupIdx int, s3client *s3.Client, tasks <-chan int, results chan<- serviceTime) {
	for i := range tasks {
		key := fmt.Sprintf("%s/%s%s%0*d", basedir, prefix, suffixseparator, suffixdigits, i)
		color.HiMagenta("DEBUG working on group [%d] file [%s/%s]", taskGroupIdx, BucketName, key)
		stopwatch := time.Now()
		time.Sleep(1 * time.Second)
		// req := s3client.GetObjectRequest(&s3.GetObjectInput{
		// 	Bucket: aws.String(BucketName),
		// 	Key:    aws.String(key),
		// })

		// resp, err := req.Send(context.Background())

		// if err != nil {
		// 	panic("Failed to get object: " + err.Error())
		// }

		firstGet := time.Now().Sub(stopwatch)

		// // create a buffer to copy the S3 object body to
		// // TODO specify somewhere the buffer size, for now it's 1MB
		// var buf = make([]byte, bufferSize)

		// // read the s3 object body into the buffer
		// size := 0
		// for {
		// 	n, err := resp.Body.Read(buf)

		// 	size += n

		// 	if err == io.EOF {
		// 		break
		// 	}

		// 	// if the streaming fails, exit
		// 	if err != nil {
		// 		// TODO see https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/handling-errors.html
		// 		panic("Error reading object body: " + err.Error())
		// 	}
		// }

		// _ = resp.Body.Close()

		lastGet := time.Now().Sub(stopwatch)

		// send measurements
		results <- serviceTime{FirstGet: firstGet, LastGet: lastGet, index: i, file: fmt.Sprintf("%s", key)}
	}
}
