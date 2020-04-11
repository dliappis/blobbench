package cmd

import "github.com/spf13/cobra"

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
	rootCmd.PersistentFlags().StringVar(&Region, "region", "us-east-2", "AWS region")
	rootCmd.PersistentFlags().StringVar(&BucketName, "bucketname", "", "AWS region")
	rootCmd.MarkFlagRequired("bucketname")
}
