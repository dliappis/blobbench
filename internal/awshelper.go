package internal

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
)

func baseCfg() aws.Config {
	// gets the AWS credentials from the default file or from the EC2 instance profile
	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		panic("Unable to load AWS SDK config: " + err.Error())
	}

	return cfg
}

// SetupS3Client helper to setup the S3 client
func SetupS3Client(region string) aws.Config {
	cfg := baseCfg()

	// set the SDK region to either the one from the program arguments or else to the same region as the EC2 instance
	cfg.Region = region

	// set a 5-minute timeout for all S3 calls, including downloading the body
	cfg.HTTPClient = &http.Client{
		Timeout: time.Minute * 10,
	}

	return cfg
}

// GetBucketRegion Returns the region for the bucket
func GetBucketRegion(bucketname string) (string, error) {
	cfg := baseCfg()

	region, err := s3manager.GetBucketRegion(context.Background(), cfg, bucketname, "us-east-2")
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			fmt.Fprintf(os.Stderr, "Unable to find region for bucket [%s]\n", bucketname)
		}
		return "", err
	}
	return region, nil
}
