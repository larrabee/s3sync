package main

import (
	"fmt"

	// "os"
	// "reflect"
	"time"

	"github.com/spf13/viper"
	"os"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

var cfg *viper.Viper
var dbConn *sql.DB

var objChan chan object
var failObjChan chan object

var sucObjCnt int
var failObjCnt int
var totalObjCnf int

func main() {
	var err error
	cfgPath := "/etc/s3dumper.conf"
	if cfg, err = GetConfig(cfgPath); err != nil {
		fmt.Printf("Cannot parse config %s error: %s", cfgPath, err)
		os.Exit(1)
	}
	if dbConn, err = sql.Open("mysql", cfg.GetString("mysql.dsn")); err != nil {
		fmt.Printf("Cannot connect to mysql: %s", err)
		os.Exit(1)
	}

	dbConn.SetMaxIdleConns(cfg.GetInt("sync.workers"))

	objChan = make(chan object, cfg.GetInt("sync.workers")*2)
	failObjChan = make(chan object, cfg.GetInt("sync.workers")*2)

	sync := syncGroup{Source: awsConn{}, Target: awsConn{}}
	if err2 := sync.Source.Configure("source"); err2 != nil {
		fmt.Printf("Connection configure failed: %s\n", err2)
		os.Exit(1)
	}

	if err2 := sync.Target.Configure("target"); err2 != nil {
		fmt.Printf("Connection configure failed: %s\n", err2)
		os.Exit(1)
	}

	sync.TableName = "sync_from_" + sync.Source.awsBucket + "_to_" + sync.Target.awsBucket

	if err = dbInit(sync.TableName); err != nil {
		fmt.Printf("Cannot init db: %s", err)
		os.Exit(1)
	}

	for i := cfg.GetInt("sync.workers"); i != 0; i-- {
		go syncObj(objChan, failObjChan)
		go processFailedObj(objChan, failObjChan)
	}

	err = sync.SyncObjToChan(objChan)
	if err != nil {
		fmt.Printf("Listing objects failed: %s\n", err)
		os.Exit(1)
	}

Wait:
	for {
		if sucObjCnt+failObjCnt == totalObjCnf {
			fmt.Println("Sync finihed sucessfully")
			break Wait
		}
		time.Sleep(time.Second)
	}

}

func GetConfig(configPath string) (*viper.Viper, error) {
	var v = viper.New()
	v.SetConfigType("yaml")
	file, err1 := os.Open(configPath)
	defer file.Close()
	if err1 != nil {
		return nil, err1
	}
	err2 := v.ReadConfig(file)
	return v, err2
}
