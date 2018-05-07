# S3Sync
#### Really fast sync tool for S3

## Features
* Multithreaded listing of buckets or local fs
* Multithreaded file downloading/uploading
* Can sync to multiple ways:
    * Bucket to local fs
    * Local fs to bucket
    * Bucket to bucket
* Retrying on errors
* Live statistics

Key future: very high speed.  
With 512 workers we get listing speed around 5k objects/sec (limited by 4 core CPU).  
With 256 workers we get avg listing and sync speed around 2k obj/sec (small objects 1-20 kb) (limited by 1Gb uplink).  

## Usage
```
>> ./s3sync --help
Really fast sync tool for S3
Version: dev, commit: none, built at: unknown
Usage: s3sync [--sk SK] [--ss SS] [--sr SR] [--se SE] [--tk TK] [--ts TS] [--tr TR] [--te TE] [--workers WORKERS] [--retry RETRY] [--rs RS] [--fe FE] [--ft FT] [--acl ACL] [--debug] [--on-fail ON-FAIL] SOURCE TARGET

Positional arguments:
  SOURCE
  TARGET

Options:
  --sk SK                Source AWS key
  --ss SS                Source AWS secret
  --sr SR                Source AWS Region [default: us-east-1]
  --se SE                Source AWS Endpoint
  --tk TK                Target AWS key
  --ts TS                Target AWS secret
  --tr TR                Target AWS Region [default: us-east-1]
  --te TE                Target AWS Endpoint
  --workers WORKERS, -w WORKERS
                         Workers count [default: 16]
  --retry RETRY, -r RETRY
                         Max numbers of retries to sync file [default: 1]
  --rs RS                Sleep interval (sec) between sync retries on error [default: 1]
  --fe FE                Sync only files with given extensions
  --ft FT                Sync only files modified after given unix timestamp
  --acl ACL              S3 ACL for uploaded files. Possible values: private, public-read, public-read-write, aws-exec-read, authenticated-read, bucket-owner-read, bucket-owner-full-control [default: private]
  --debug, -d            Show debug logging
  --on-fail ON-FAIL, -f ON-FAIL
                         Action on failed. Possible values: fatal, log [default: fatal]
  --help, -h             display this help and exit
  --version              display version and exit
```

## Install
Download binary from [Release page](https://github.com/larrabee/s3sync/releases).  

## Building
Build it with `go build`.
You can use `dep ensure` for vendored dependencies.

## License
GPLv3
