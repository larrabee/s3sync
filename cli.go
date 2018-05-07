package main

import (
	"github.com/alexflint/go-arg"
	"net/url"
	"strings"
	"time"
	"fmt"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

type ConnType int
type OnFailAction int

const (
	S3Conn ConnType = 1
	FSConn ConnType = 2

	OnFailFatal OnFailAction = 1
	OnFailLog OnFailAction = 2
)

type ArgsParsed struct {
	Args
	Source             Connect
	Target             Connect
	RetrySleepInterval time.Duration
	OnFail OnFailAction
}

type Connect struct {
	Type   ConnType
	Bucket string
	Path   string
}

// Args cli args structure
type Args struct {
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
	Workers            uint     `arg:"-w" help:"Workers count"`
	Retry              uint     `arg:"-r" help:"Max numbers of retries to sync file"`
	RetrySleepInterval uint     `arg:"--rs" help:"Sleep interval (sec) between sync retries on error"`
	FilterExtension    []string `arg:"--fe,separate" help:"Sync only files with given extensions"`
	FilterTimestamp    int64    `arg:"--ft" help:"Sync only files modified after given unix timestamp"`
	Acl                string   `arg:"--acl" help:"S3 ACL for uploaded files. Possible values: private, public-read, public-read-write, aws-exec-read, authenticated-read, bucket-owner-read, bucket-owner-full-control"`
	Debug              bool     `arg:"-d" help:"Show debug logging"`
	OnFail             string   `arg:"--on-fail,-f" help:"Action on failed. Possible values: fatal, log"`
}

func (Args) Version() string {
	return fmt.Sprintf("Version: %v, commit: %v, built at: %v", version, commit, date)
}

func (Args) Description() string {
	return "Really fast sync tool for S3"
}

// GetCliArgs return cli args structure
func GetCliArgs() (cli ArgsParsed, err error) {
	rawCli := Args{}
	rawCli.SourceRegion = "us-east-1"
	rawCli.TargetRegion = "us-east-1"
	rawCli.Workers = 16
	rawCli.Retry = 1
	rawCli.RetrySleepInterval = 1
	rawCli.Acl = "private"
	rawCli.OnFail = "fatal"

	p := arg.MustParse(&rawCli)
	cli.Args = rawCli

	switch cli.Args.Acl {
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

	switch cli.Args.OnFail {
	case "fatal":
		cli.OnFail = OnFailFatal
	case "log":
		cli.OnFail = OnFailLog
	default:
		p.Fail("--on-fail must be one of \"fatal, log\"")

	}

	cli.RetrySleepInterval = time.Duration(cli.Args.RetrySleepInterval) * time.Second
	if cli.Source, err = ParseConn(cli.Args.Source); err != nil {
		return cli, err
	}
	if cli.Target, err = ParseConn(cli.Args.Target); err != nil {
		return cli, err
	}
	return
}

func ParseConn(cStr string) (conn Connect, err error) {
	u, err := url.Parse(cStr)
	if err != nil {
		return
	}

	switch u.Scheme {
	case "s3":
		conn.Type = S3Conn
		conn.Bucket = u.Host
		conn.Path = strings.TrimPrefix(u.Path, "/")
	default:
		conn.Type = FSConn
		conn.Path = cStr
	}
	return
}
