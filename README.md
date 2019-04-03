# S3Sync
#### Really fast sync tool for S3
[![Go Report Card](https://goreportcard.com/badge/github.com/larrabee/s3sync)](https://goreportcard.com/report/github.com/larrabee/s3sync)  

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
Version: 1.12, commit: 36269e3514d5d1c19d40f6df8cb76e41d670da32, built at: 2019-04-03T07:38:43Z
Usage: s3sync [--sk SK] [--ss SS] [--sr SR] [--se SE] [--tk TK] [--ts TS] [--tr TR] [--te TE] [--workers WORKERS] [--retry RETRY] [--rs RS] [--fe FE] [--ft FT] [--acl ACL] [--debug] [--on-fail ON-FAIL] [--disable-http2] SOURCE TARGET

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
                         Action on failed. Possible values: fatal, log, ignoremissing [default: fatal]
  --disable-http2        Disable HTTP2 for http client
  --help, -h             display this help and exit
  --version              display version and exit
```

Examples:  
* Sync Amazon S3 bucket to FS:  
```s3sync --sk KEY --ss SECRET -w 128 s3://shared /opt/backups/s3/ -d -r 10```
* Sync S3 bucket with custom endpoint to FS:  
```s3sync --sk KEY --ss SECRET --se "http://127.0.0.1:7484" -w 128 s3://shared /opt/backups/s3/ -d -r 10```
* Sync directory (/test) from Amazon S3 bucket to FS:  
```s3sync --sk KEY --ss SECRET -w 128 s3://shared/test /opt/backups/s3/test/ -d -r 10```
* Sync directory from local FS to Amazon S3:  
```s3sync --tk KEY --ts SECRET -w 128 /opt/backups/s3/ s3://shared -d -r 10```
* Sync directory from local FS to Amazon S3 bucket directory:  
```s3sync --tk KEY --ts SECRET -w 128 /opt/backups/s3/test/ s3://shared/test_new/ -d -r 10```
* Sync one Amazon bucket to another Amazon bucket:  
```s3sync --tk KEY2 --ts SECRET2 --sk KEY1 --ss SECRET1 -w 128 s3://shared s3://shared_new -d -r 10```
* Sync S3 bucket with custom endpoint to another bucket with custom endpoint:  
```s3sync --tk KEY2 --ts SECRET2 --sk KEY1 --ss SECRET1 --se "http://127.0.0.1:7484" --te "http://127.0.0.1:7484" -w 128 s3://shared s3://shared_new -d -r 10```
* Sync one Amazon bucket directory to another Amazon bucket:  
```s3sync --tk KEY2 --ts SECRET2 --sk KEY1 --ss SECRET1 -w 128 s3://shared/test/ s3://shared_new -d -r 10```

SOURCE and TARGET should be a directory. Syncing of single file are not supported (This will not work `s3sync --sk KEY --ss SECRET s3://shared/megafile.zip /opt/backups/s3/`)  

You can use filters.   
Timestamp filter (`--ft` key) syncing only files, that has been changed after specified date. Its useful for diff backups.  
File extension filter (`--fe` key) syncing only files, that have specified extension. Can be specified multiple times (Like this `--fe .jpg --fe .png --fe .bmp`).  

## Install
Download binary from [Release page](https://github.com/larrabee/s3sync/releases).  

## Building
Build it with `go build`.
You can use `dep ensure` for vendored dependencies.

## License
GPLv3
