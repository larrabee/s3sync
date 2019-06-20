package main

import (
	"fmt"
	"github.com/alexflint/go-arg"
	"github.com/larrabee/s3sync/storage"
	"github.com/mattn/go-isatty"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

type onFailAction int

const (
	onFailFatal onFailAction = iota
	onFailSkip
	onFailSkipMissing
)

type argsParsed struct {
	args
	Source          connect
	Target          connect
	S3RetryInterval time.Duration
	OnFail          onFailAction
	FSFilePerm      os.FileMode
	FSDirPerm       os.FileMode
}

type connect struct {
	Type   storage.Type
	Bucket string
	Path   string
}

type args struct {
	// Source config
	Source         string `arg:"positional"`
	SourceKey      string `arg:"--sk" help:"Source AWS key"`
	SourceSecret   string `arg:"--ss" help:"Source AWS secret"`
	SourceRegion   string `arg:"--sr" help:"Source AWS Region"`
	SourceEndpoint string `arg:"--se" help:"Source AWS Endpoint"`
	// Target config
	Target         string `arg:"positional"`
	TargetKey      string `arg:"--tk" help:"Target AWS key"`
	TargetSecret   string `arg:"--ts" help:"Target AWS secret"`
	TargetRegion   string `arg:"--tr" help:"Target AWS Region"`
	TargetEndpoint string `arg:"--te" help:"Target AWS Endpoint"`
	// S3 config
	S3Retry         uint   `arg:"--s3-retry" help:"Max numbers of retries to sync file"`
	S3RetryInterval uint   `arg:"--s3-retry-sleep" help:"Sleep interval (sec) between sync retries on error"`
	S3Acl           string `arg:"--s3-acl" help:"S3 ACL for uploaded files. Possible values: private, public-read, public-read-write, aws-exec-read, authenticated-read, bucket-owner-read, bucket-owner-full-control"`
	S3KeysPerReq    int64  `arg:"--s3-keys-per-req" help:"Max numbers of keys retrieved via List request"`
	// FS config
	FSFilePerm string `arg:"--fs-file-perm" help:"File permissions"`
	FSDirPerm  string `arg:"--fs-dir-perm" help:"Dir permissions"`
	// Filters
	FilterExt         []string `arg:"--filter-ext,separate" help:"Sync only files with given extensions"`
	FilterExtNot      []string `arg:"--filter-not-ext,separate" help:"Skip files with given extensions"`
	FilterCT          []string `arg:"--filter-ct,separate" help:"Sync only files with given Content-Type"`
	FilterCTNot       []string `arg:"--filter-not-ct,separate" help:"Skip files with given Content-Type"`
	FilterMtimeAfter  int64    `arg:"--filter-after-mtime" help:"Sync only files modified after given unix timestamp"`
	FilterMtimeBefore int64    `arg:"--filter-before-mtime" help:"Sync only files modified before given unix timestamp"`
	// Misc
	Workers      uint   `arg:"-w" help:"Workers count"`
	Debug        bool   `arg:"-d" help:"Show debug logging"`
	SyncLog      bool   `arg:"--sync-log" help:"Show sync log"`
	ShowProgress bool   `arg:"--sync-progress,-p" help:"Show sync progress"`
	OnFail       string `arg:"--on-fail,-f" help:"Action on failed. Possible values: fatal, skip, skipmissing"`
	DisableHTTP2 bool   `arg:"--disable-http2" help:"Disable HTTP2 for http client"`
	ListBuffer      uint   `arg:"--list-buffer" help:"Size of list buffer"`
}

//VersionId return program version string on human format
func (args) Version() string {
	return fmt.Sprintf("VersionId: %v, commit: %v, built at: %v", version, commit, date)
}

//Description return program description string
func (args) Description() string {
	return "Really fast sync tool for S3"
}

//GetCliArgs return cli args structure and error
func GetCliArgs() (cli argsParsed, err error) {
	rawCli := args{}
	rawCli.SourceRegion = "us-east-1"
	rawCli.TargetRegion = "us-east-1"
	rawCli.Workers = 16
	rawCli.S3Retry = 0
	rawCli.S3RetryInterval = 0
	rawCli.S3Acl = "private"
	rawCli.S3KeysPerReq = 1000
	rawCli.OnFail = "fatal"
	rawCli.FSDirPerm = "0755"
	rawCli.FSFilePerm = "0644"
	rawCli.ListBuffer = 1000

	p := arg.MustParse(&rawCli)
	cli.args = rawCli

	switch cli.args.S3Acl {
	case "private":
		break
	case "public-read":
		break
	case "public-read-write":
		break
	case "aws-exec-read":
		break
	case "authenticated-read":
		break
	case "bucket-owner-read":
		break
	case "bucket-owner-full-control":
		break
	default:
		p.Fail("--acl must be one of \"private, public-read, public-read-write, aws-exec-read, authenticated-read, bucket-owner-read, bucket-owner-full-control\"")
	}

	switch cli.args.OnFail {
	case "fatal":
		cli.OnFail = onFailFatal
	case "skip":
		cli.OnFail = onFailSkip
	case "skipmissing":
		cli.OnFail = onFailSkipMissing
	default:
		p.Fail("--on-fail must be one of \"fatal, skip, skipmissing\"")
	}

	cli.S3RetryInterval = time.Duration(cli.args.S3RetryInterval) * time.Second
	if cli.Source, err = parseConn(cli.args.Source); err != nil {
		return cli, err
	}
	if cli.Target, err = parseConn(cli.args.Target); err != nil {
		return cli, err
	}
	if cli.args.ShowProgress && !isatty.IsTerminal(os.Stdout.Fd()) {
		p.Fail("Progress (--sync-progress) require tty")
	}

	if filePerm, err := strconv.ParseUint(cli.args.FSFilePerm, 8, 32); err != nil {
		p.Fail("Failed to parse arg --fs-file-perm")
	} else {
		cli.FSFilePerm = os.FileMode(filePerm)
	}

	if dirPerm, err := strconv.ParseUint(cli.args.FSDirPerm, 8, 32); err != nil {
		p.Fail("Failed to parse arg --fs-dir-perm")
	} else {
		cli.FSDirPerm = os.FileMode(dirPerm)
	}

	if cli.DisableHTTP2 {
		_ = os.Setenv("GODEBUG", os.Getenv("GODEBUG")+"http2client=0")
	}

	return
}

func parseConn(cStr string) (conn connect, err error) {
	u, err := url.Parse(cStr)
	if err != nil {
		return conn, err
	}

	switch u.Scheme {
	case "s3":
		conn.Type = storage.TypeS3
		conn.Bucket = u.Host
		conn.Path = strings.TrimPrefix(u.Path, "/")
	case "fs":
		conn.Type = storage.TypeFS
		conn.Path = u.Host + u.Path
	default:
		conn.Type = storage.TypeFS
		conn.Path = cStr
	}
	return
}
