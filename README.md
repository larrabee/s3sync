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

## Install
Download binary from [Release page](https://github.com/larrabee/s3sync/releases).  

## Building
Build it with `go build`.
You can use `dep ensure` for vendored dependencies.

## License
GPLv3
