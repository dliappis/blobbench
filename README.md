# Blobbench

## Overview

I developed this tool to help me benchmark the achieved *download* performance of various blob store engines (S3, GCS, Azure Storage).

The tool currently has two subcommands `upload` and `download`.
The former is very limited and basically is a convenience tool for uploading a file using multipart on AWS S3 with a user defined `partsize`.

The `download` subcommand at the moment only support AWS S3 and supports streaming an arbitrary number of files from a bucket using a configurable amount of concurrency (simulates a threadpool) producing a report of achieved throughput per file.

Some parts were inspired by Daniel Vassallo's [s3-benchmark](https://github.com/dvassallo/s3-benchmark).

## Building

Requires a correctly set up GO environment.
Just clone this repo and run `make`.

It will generate the following binaries:

```
build
├── blobbench_darwin_amd64
└── blobbench_linux_amd64
```

You can use one of them depending on your platform.
Windows support is missing atm, should be easy to add in the future.

## CLI usage example

Suppose you have `1024` files stored on the S3 bucket `mybucket` under `mydirectory/` using the following pattern:

- `file-0000`
- `file-0001`
- `file-0002`
...
- `file-1023`

Assuming your AWS credentials are correct (basically `AWS_PROFILE` env var correctly set) you can test the download performance with 5 parallel workers using:

`build/blobbench_linux_amd64 --bucketname mybucket download --basedir mydirectory --prefix file --numfiles 1024 --workers 5`

There are more parameters supported by the download subcommand, see below section.

## Download command

The download command streams (using the AWS [GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html) API) and calculates the achived throughput per file; it doesn't *actually* use any File IO to write to the filesystem as the intention is to benchmark the performance of the blobstore and not of the local filesystem.

The number of files can be specified with `--numfiles` and need to use a suffix starting with `0`. The actual number of decimal digits is configurable using `--suffixdigits`. For files like `testfile-000`, `testfile-001`, ... you need to use `--suffixdigits 3`.

You can also specify the separator between the fileprefix and the suffix using `--suffixseparator`. By defaults it's `-`.

The download buffer, per worker, is configurable as well; it defaults to 1KB and can be configured using `-bufsize`.

Finally the number of parallel workers can be configured using `--workers` (default is 5). This simulates how a threadpool would work: say you specified 5 files (`file-00` ... `file-04`) and 2 workers.

The download schedule will be:

| Start time | Worker # | File | Duration |
| ---------- | -------- | ---- | ---------------- |
| t0         | 1        | file-00 | dur0          |
| t0         | 2        | file-01 | dur1          |

... assuming dur0 < dur1, worked 1 finishes first ...

| t0+dur0 | 1 | file-02 | dur2 |

... here worker 2 finishes ...

| t0+dur1 | 2 | file-03 | dur3 |

... and again dur2<dur3 so worker 1 finishes first and downloads the final file ...

| t0+dur0+dur2 | 1 file-04 | dur4 |

## Reports

By default metrics for each downloaded file will be printed to stdout.
This can be changed using the global parameter `--output`.

## Generating a random dataset

This is not currently done with this tool but you can utilize e.g. the `dd` command reading from `/dev/urandom`. For example to create 1TB of random data:

`dd if=/dev/urandom bs=1M count=1M of=terradump`

To split into 1024 1GB files: `split -d -b1G -a4 terradump terradump-`
