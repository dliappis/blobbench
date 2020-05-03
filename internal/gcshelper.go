package internal

import (
	"context"

	"cloud.google.com/go/storage"
)

// SetupGCSClient helper to setup the GCS client
func SetupGCSClient() *storage.Client {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		panic("Failed to create client: " + err.Error())
	}
	return client
}
