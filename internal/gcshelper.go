package internal

import (
	"context"
	"fmt"
	"reflect"
	"unsafe"

	"cloud.google.com/go/storage"
)

// SetupGCSClient helper to setup the GCS client
func SetupGCSClient() *storage.Client {
	ctx := context.Background()

	// hc, _ := google.DefaultClient(ctx, storage.ScopeReadOnly)

	client, err := storage.NewClient(ctx)

	if err != nil {
		panic("Failed to create client: " + err.Error())
	}

	c := reflect.ValueOf(client).Elem()
	hc := c.FieldByName("hc").Elem()
	fmt.Printf("HC: %#v\n", hc)
	tp := hc.FieldByName("Transport").Elem()

	base := reflect.Indirect(tp).FieldByName("Base").Elem()
	octp := reflect.Indirect(base).FieldByName("Base").Elem()
	realtp := reflect.Indirect(octp).FieldByName("base").Elem()

	fmt.Printf("HC: %#v\n", tp)
	fmt.Printf("HC: %#v\n", base)
	fmt.Printf("HC: %#v\n", octp)
	fmt.Printf("HC: %#v\n", realtp)

	bufSize := reflect.Indirect(realtp).FieldByName("ReadBufferSize")

	fmt.Printf("HC: %#v\n", bufSize)

	rf := reflect.NewAt(bufSize.Type(), unsafe.Pointer(bufSize.UnsafeAddr())).Elem()
	rf.SetInt(2 * 1024 * 1024)
	fmt.Printf("HC: %#v\n", bufSize)

	return client
}
