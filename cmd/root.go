package cmd

import (
	"github.com/spf13/cobra"
)

var defaultRegion = "us-east-2"

// Region ...
var Region string

// BucketName ...
var BucketName string

// Provider ...
var Provider string

// OutputFile is the filename where results will be written
var OutputFile string

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
	rootCmd.PersistentFlags().StringVar(&BucketName, "bucketname", "", "The name of the bucket")
	rootCmd.MarkFlagRequired("bucketname")
	rootCmd.PersistentFlags().StringVar(&Region, "region", defaultRegion, "region")
	rootCmd.MarkFlagRequired("provider")
	rootCmd.PersistentFlags().StringVar(&Provider, "provider", "", "Specifies the provider (aws, gcp, azure, dummy)")
	rootCmd.PersistentFlags().StringVar(&OutputFile, "output", "", "Stores results to the specified file")
}
