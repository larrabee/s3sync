# S3Sync
#### Really fast sync tool for S3
[![Go Report Card](https://goreportcard.com/badge/github.com/larrabee/s3sync)](https://goreportcard.com/report/github.com/larrabee/s3sync) [![GoDoc](https://godoc.org/github.com/larrabee/s3sync?status.svg)](https://godoc.org/github.com/larrabee/s3sync) [![Build Status](https://travis-ci.org/larrabee/s3sync.svg?branch=master)](https://travis-ci.org/larrabee/s3sync)  

## Features
* Multi-threaded file downloading/uploading
* Can sync to multiple ways:
    * S3 to local FS
    * Local FS to S3
    * S3 to S3
* Retrying on errors
* Live statistics
* Rate limiting by objects
* Rate limiting by bandwidth
* Flexible filters by extension, Content-Type, ETag and object mtime

Key feature: very high speed.  
Avg listing speed around 5k objects/sec for S3.  
With 128 workers we get avg sync speed around 2k obj/sec (small objects 1-20 kb) (limited by 1Gb uplink).  

## Limitations
* Each object is loaded into RAM. So you need `<avg object size> * <workers count>` RAM.  
  If you don't have enough RAM, you can use swap. A large (32-64 Gb) swap on SSD does not affect the tool performance.  
  This happened because the tool was designed to synchronize billions of small files and optimized for this workload.

## Usage
```
>> s3sync --help
Really fast sync tool for S3
VersionId: dev, commit: none, built at: unknown
Usage: cli [--sn] [--sk SK] [--ss SS] [--st ST] [--sr SR] [--se SE] [--tn] [--tk TK] [--ts TS] [--tt TT] [--tr TR] [--te TE] [--s3-retry S3-RETRY] [--s3-retry-sleep S3-RETRY-SLEEP] [--s3-acl S3-ACL] [--s3-storage-class S3-STORAGE-CLASS] [--s3-keys-per-req S3-KEYS-PER-REQ] [--fs-file-perm FS-FILE-PERM] [--fs-dir-perm FS-DIR-PERM] [--fs-disable-xattr] [--fs-atomic-write] [--filter-ext FILTER-EXT] [--filter-not-ext FILTER-NOT-EXT] [--filter-ct FILTER-CT] [--filter-not-ct FILTER-NOT-CT] [--filter-after-mtime FILTER-AFTER-MTIME] [--filter-before-mtime FILTER-BEFORE-MTIME] [--filter-modified] [--filter-exist] [--filter-not-exist] [--filter-dirs] [--filter-not-dirs] [--workers WORKERS] [--debug] [--sync-log] [--sync-log-format SYNC-LOG-FORMAT] [--sync-progress] [--on-fail ON-FAIL] [--error-handling ERROR-HANDLING] [--disable-http2] [--list-buffer LIST-BUFFER] [--ratelimit-objects RATELIMIT-OBJECTS] [--ratelimit-bandwidth RATELIMIT-BANDWIDTH] SOURCE TARGET

Positional arguments:
  SOURCE
  TARGET

Options:
  --sn                   Don't sign request to source AWS for anonymous access
  --sk SK                Source AWS key
  --ss SS                Source AWS session secret
  --st ST                Source AWS token
  --sr SR                Source AWS Region
  --se SE                Source AWS Endpoint
  --tn                   Don't sign request to target AWS for anonymous access
  --tk TK                Target AWS key
  --ts TS                Target AWS secret
  --tt TT                Target AWS session token
  --tr TR                Target AWS Region
  --te TE                Target AWS Endpoint
  --s3-retry S3-RETRY    Max numbers of retries to sync file
  --s3-retry-sleep S3-RETRY-SLEEP
                         Sleep interval (sec) between sync retries on error
  --s3-acl S3-ACL        S3 ACL for uploaded files. Possible values: private, public-read, public-read-write, aws-exec-read, authenticated-read, bucket-owner-read, bucket-owner-full-control
  --s3-storage-class S3-STORAGE-CLASS
                         S3 Storage Class for uploaded files.
  --s3-keys-per-req S3-KEYS-PER-REQ
                         Max numbers of keys retrieved via List request [default: 1000]
  --fs-file-perm FS-FILE-PERM
                         File permissions [default: 0644]
  --fs-dir-perm FS-DIR-PERM
                         Dir permissions [default: 0755]
  --fs-disable-xattr     Disable FS xattr for storing metadata
  --fs-atomic-write      Enable FS atomic writes. New files will be written to temp file and renamed
  --filter-ext FILTER-EXT
                         Sync only files with given extensions
  --filter-not-ext FILTER-NOT-EXT
                         Skip files with given extensions
  --filter-ct FILTER-CT
                         Sync only files with given Content-Type
  --filter-not-ct FILTER-NOT-CT
                         Skip files with given Content-Type
  --filter-after-mtime FILTER-AFTER-MTIME
                         Sync only files modified after given unix timestamp
  --filter-before-mtime FILTER-BEFORE-MTIME
                         Sync only files modified before given unix timestamp
  --filter-modified      Sync only modified files
  --filter-exist         Sync only files, that exist in target storage
  --filter-not-exist     Sync only files, that doesn't exist in target storage
  --filter-dirs          Sync only files, that ends with slash (/)
  --filter-not-dirs      Skip files that ends with slash (/)
  --workers WORKERS, -w WORKERS
                         Workers count [default: 16]
  --debug, -d            Show debug logging
  --sync-log             Show sync log
  --sync-log-format SYNC-LOG-FORMAT
                         Format of sync log. Possible values: json
  --sync-progress, -p    Show sync progress
  --on-fail ON-FAIL, -f ON-FAIL
                         Action on failed. Possible values: fatal, skip, skipmissing (DEPRECATED, use --error-handling instead) [default: fatal]
  --error-handling ERROR-HANDLING
                         Controls error handling. Sum of the values: 1 for ignoring NotFound errors, 2 for ignoring PermissionDenied errors OR 255 to ignore all errors
  --disable-http2        Disable HTTP2 for http client
  --list-buffer LIST-BUFFER
                         Size of list buffer [default: 1000]
  --ratelimit-objects RATELIMIT-OBJECTS
                         Rate limit objects per second
  --ratelimit-bandwidth RATELIMIT-BANDWIDTH
                         Set bandwidth rate limit, byte/s, Allow suffixes: K, M, G
  --help, -h             display this help and exit
  --version              display version and exit
```

Examples:  
* Sync Amazon S3 bucket to FS:  
```s3sync --sk KEY --ss SECRET -w 128 s3://shared fs:///opt/backups/s3/```
* Sync S3 bucket with custom endpoint to FS:  
```s3sync --sk KEY --ss SECRET --se "http://127.0.0.1:7484" -w 128 s3://shared fs:///opt/backups/s3/```
* Sync directory (/test) from Amazon S3 bucket to FS:  
```s3sync --sk KEY --ss SECRET -w 128 s3://shared/test fs:///opt/backups/s3/test/```
* Sync directory from local FS to Amazon S3:  
```s3sync --tk KEY --ts SECRET -w 128 fs:///opt/backups/s3/ s3://shared```
* Sync directory from local FS to Amazon S3 bucket directory:  
```s3sync --tk KEY --ts SECRET -w 128 fs:///opt/backups/s3/test/ s3://shared/test_new/```
* Sync one Amazon bucket to another Amazon bucket:  
```s3sync --tk KEY2 --ts SECRET2 --sk KEY1 --ss SECRET1 -w 128 s3://shared s3://shared_new```
* Sync S3 bucket with custom endpoint to another bucket with custom endpoint:  
```s3sync --tk KEY2 --ts SECRET2 --sk KEY1 --ss SECRET1 --se "http://127.0.0.1:7484" --te "http://127.0.0.1:7484" -w 128 s3://shared s3://shared_new```
* Sync one Amazon bucket directory to another Amazon bucket:  
```s3sync --tk KEY2 --ts SECRET2 --sk KEY1 --ss SECRET1 -w 128 s3://shared/test/ s3://shared_new```
* Use streaming S3 transfers (reduces memory, might be slower for small files):
```s3sync --sk KEY --ss SECRET --tk KEY --ts SECRET -w 128 s3s://shared/test/ s3s://shared_new```

SOURCE and TARGET should be a directory. Syncing of single file are not supported (This will not work `s3sync --sk KEY --ss SECRET s3://shared/megafile.zip fs:///opt/backups/s3/`)  

You can use filters.   
* Timestamp filter (`--filter-after-mtime` arg) syncing only files, that has been changed after specified timestamp. Its useful for diff backups.  
* File extension filter (`--filter-ext` arg) syncing only files, that have specified extension. Can be specified multiple times (Like this `--filter-ext .jpg --filter-ext .png --filter-ext .bmp`).
* Content-type filter (`--filter-ct` arg) syncing only files, that have specified content-type. Can be specified multiple times.
* Etag filter (`--filter-modified`) sync only modified files. It have few restrictions. If you are using FS storage, the files must be created using s3sync. FS storage should also support xattr.
* There are also inverted filters (`--filter-not-ext`, `--filter-not-ct` and `--filter-before-mtime`).

## Install
Download binary from [Release page](https://github.com/larrabee/s3sync/releases).  
Or use docker image [larrabee/s3sync](https://hub.docker.com/repository/docker/larrabee/s3sync) like this:  
```
docker run --rm -ti larrabee/s3sync --tk KEY2 --ts SECRET2 --sk KEY1 --ss SECRET1 -w 128 s3://shared/test/ s3://shared_new
```

## Building
Minimum go version: **1.13**  
Build it with:
 ```
go mod vendor
go build -o s3sync ./cli 
 ```

## Using module
You can easy use s3sync in your application. See example in `cli/` folder. 

## License
GPLv3

## Notes
s3sync is a non-destructive one-way sync: it does not delete files in the destination or source paths that are out of sync.
