package cmd

import (
	"github.com/spf13/cobra"
)

var defaultRegion = "us-east-2"

// Region ...
var Region string

// BucketName ...
var BucketName string

var (
	userLicense string

	rootCmd = &cobra.Command{
		Use:   "blobbench",
		Short: "benchmarking tool for blob stores",
		Long:  `TO DO`,
	}
)

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVar(&BucketName, "bucketname", "", "AWS region")
	rootCmd.MarkFlagRequired("bucketname")
	rootCmd.PersistentFlags().StringVar(&Region, "region", defaultRegion, "AWS region")
}

// TODO figure out where to do autoregion
// func setup(cmd *cobra.Command, args []string) {
// 	if Region == "" {
// 		color.Yellow("No region specified. Attempting to determine automatically.")
// 		if autoregion, err := internal.GetBucketRegion(BucketName); err != nil {
// 			color.Green("Determined region for bucket [%s] is [%s]", BucketName, autoregion)
// 			Region = autoregion
// 		} else {
// 			color.Red("Unable to automatically find region for bucket. Asssuming %s.", defaultRegion)
// 			Region = defaultRegion
// 		}
// 	}
// }
