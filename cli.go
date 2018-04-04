package main

import (
	"github.com/alexflint/go-arg"
	"net/url"
	"strings"
	"time"
)

type ConnType int

const (
	S3Conn ConnType = 1
	FSConn ConnType = 2
)

type ArgsParsed struct {
	Args
	Source             Connect
	Target             Connect
	RetrySleepInterval time.Duration
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
	SourceKey      string `arg:"--source-key,--sk" help:"Source AWS key"`
	SourceSecret   string `arg:"--source-secret,--ss" help:"Source AWS secret"`
	SourceRegion   string `arg:"--source-region,--sr" help:"Source AWS Region"`
	SourceEndpoint string `arg:"--source-endpoint,--se" help:"Source AWS Endpoint"`
	// Target config
	Target         string `arg:"positional"`
	TargetKey      string `arg:"--target-key,--tk" help:"Target AWS key"`
	TargetSecret   string `arg:"--target-secret,--ts" help:"Target AWS secret"`
	TargetRegion   string `arg:"--target-region,--tr" help:"Target AWS Region"`
	TargetEndpoint string `arg:"--target-endpoint,--te" help:"Target AWS Endpoint"`
	// Sync config
	Workers            uint     `arg:"-w" help:"Workers count"`
	Retry              uint     `arg:"-r" help:"Max numbers of retry to syncGr file"`
	RetrySleepInterval uint     `arg:"--rs" help:"Sleep interval (sec) between sync retries on error"`
	FilterExtension    []string `arg:"--filter-extension,--fe" help:"Sync only files with given extensions"`
	FilterTimestamp    int64    `arg:"--filter-timestamp,--ft" help:"Sync only files modified after given unix timestamp"`
	Debug              bool     `arg:"-d" help:"Show debug logging"`
}

// GetCliArgs return cli args structure
func GetCliArgs() (cli ArgsParsed, err error) {
	rawCli := Args{}
	rawCli.SourceRegion = "us-east-1"
	rawCli.TargetRegion = "us-east-1"
	rawCli.Workers = 16
	rawCli.Retry = 1
	rawCli.RetrySleepInterval = 1

	arg.MustParse(&rawCli)
	cli.Args = rawCli

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
