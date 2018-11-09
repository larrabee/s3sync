package main

import (
	"fmt"
	"github.com/alexflint/go-arg"
	"net/url"
	"strings"
	"time"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

type ConnType int
type onFailAction int

const (
	s3Conn   ConnType = 1
	fsConn   ConnType = 2
	s3StConn ConnType = 3

	onFailFatal onFailAction = 1
	onFailLog   onFailAction = 2
)

type argsParsed struct {
	args
	Source        connect
	Target        connect
	RetryInterval time.Duration
	OnFail        onFailAction
}

type connect struct {
	Type   ConnType
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
	// Sync config
	Workers         uint     `arg:"-w" help:"Workers count"`
	Retry           uint     `arg:"-r" help:"Max numbers of retries to sync file"`
	RetryInterval   uint     `arg:"--rs" help:"Sleep interval (sec) between sync retries on error"`
	FilterExtension []string `arg:"--fe,separate" help:"Sync only files with given extensions"`
	FilterTimestamp int64    `arg:"--ft" help:"Sync only files modified after given unix timestamp"`
	Acl             string   `arg:"--acl" help:"S3 ACL for uploaded files. Possible values: private, public-read, public-read-write, aws-exec-read, authenticated-read, bucket-owner-read, bucket-owner-full-control"`
	Debug           bool     `arg:"-d" help:"Show debug logging"`
	OnFail          string   `arg:"--on-fail,-f" help:"Action on failed. Possible values: fatal, log"`
	DisableHTTP2    bool     `arg:"--disable-http2" help:"Disable HTTP2 for http client"`
}

//Version return program version string on human format
func (args) Version() string {
	return fmt.Sprintf("Version: %v, commit: %v, built at: %v", version, commit, date)
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
	rawCli.Retry = 1
	rawCli.RetryInterval = 1
	rawCli.Acl = "private"
	rawCli.OnFail = "fatal"

	p := arg.MustParse(&rawCli)
	cli.args = rawCli

	switch cli.args.Acl {
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
	case "log":
		cli.OnFail = onFailLog
	default:
		p.Fail("--on-fail must be one of \"fatal, log\"")
	}

	cli.RetryInterval = time.Duration(cli.args.RetryInterval) * time.Second
	if cli.Source, err = parseConn(cli.args.Source); err != nil {
		return cli, err
	}
	if cli.Target, err = parseConn(cli.args.Target); err != nil {
		return cli, err
	}
	return
}

func parseConn(cStr string) (conn connect, err error) {
	u, err := url.Parse(cStr)
	if err != nil {
		return
	}

	switch u.Scheme {
	case "s3":
		conn.Type = s3Conn
		conn.Bucket = u.Host
		conn.Path = strings.TrimPrefix(u.Path, "/")
	case "s3st":
		conn.Type = s3StConn
		conn.Bucket = u.Host
		conn.Path = strings.TrimPrefix(u.Path, "/")
	default:
		conn.Type = fsConn
		conn.Path = cStr
	}
	return
}
