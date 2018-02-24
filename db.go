package main

import (
	"database/sql"
	"fmt"
	"os"
)

func dbInit(connName string) error {
	bucketName := cfg.GetString(fmt.Sprintf("connections.%s.bucketName", connName))
	if _, err := dbConn.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `awssync_%s` ("+
		"`key` TEXT NOT NULL,"+
		"`etag` varchar(255) NOT NULL,"+
		"PRIMARY KEY (`key`(3072))"+
		")", bucketName)); err != nil {
		return err
	}

	if _, err := dbConn.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `awssync_%s_err` ("+
		"`key` TEXT NOT NULL,"+
		"PRIMARY KEY (`key`(3072))"+
		")", bucketName)); err != nil {
		return err
	}

	return nil
}

func dbGetObjEtag(obj object) (etag string, err error) {
	bucketName := obj.SyncGroup.Source.awsBucket
  //var stmt *sql.Stmt
  stmt, err := dbConn.Prepare(fmt.Sprintf("SELECT `etag` FROM `awssync_%s` WHERE `key`=?", bucketName))
  if err != nil {
    return etag, err
  }
	err = stmt.QueryRow(obj.Key).Scan(&etag)
  defer stmt.Close()
	switch {
	case err == sql.ErrNoRows:
		return etag, nil
	default:
		return etag, err
	}
}

func dbAddObj(obj object) (err error) {
	bucketName := obj.SyncGroup.Source.awsBucket
  stmt, err := dbConn.Prepare(fmt.Sprintf("INSERT INTO `awssync_%s` (`key`, `etag`) VALUES( ?, ? ) ON DUPLICATE KEY UPDATE `etag`=VALUES(`etag`)", bucketName))
  if err != nil {
    return err
  }
	_, err = stmt.Exec(obj.Key, obj.ETag) 
  defer stmt.Close()
  if err != nil {
		return err
	}
	return nil
}

func processFailedObj(ch chan<- object, failCh <-chan object) {
Main:
	for obj := range failCh {
		if obj.ErrCount == 0 {
			bucketName := obj.SyncGroup.Source.awsBucket
      stmt, err := dbConn.Prepare(fmt.Sprintf("INSERT IGNORE INTO `awssync_%s_err` (key), VALUES(?)", bucketName))
      if err != nil {
        fmt.Printf("INSERT failObj to DB failed: %s, Terminating...", err)
				os.Exit(1)
      }
      
			_, err = stmt.Exec(obj.Key);
      if err != nil {
				fmt.Printf("INSERT failObj to DB failed: %s, Terminating...", err)
				os.Exit(1)
			}
			failObjCnt++
			continue Main
		}
		obj.ErrCount--
		ch <- obj
	}
}

func syncObj(ch chan object, failCh chan<- object) {
Main:
	for obj := range ch {
		if etag, err := dbGetObjEtag(obj); etag != obj.ETag {
			if err != nil {
				fmt.Printf("Etag getting error: %s\n", err)
				failCh <- obj
				continue Main
			}
			if err := obj.GetContent(); err != nil {
				fmt.Printf("Get content error: %s\n",err)
				failCh <- obj
				continue Main
			}
			if err := obj.PutContent(); err != nil {
				fmt.Printf("Put content error: %s\n",err)
				failCh <- obj
				continue Main
			}
			if err := dbAddObj(obj); err != nil {
				fmt.Printf("DB object add error: %s\n",err)
				failCh <- obj
				continue Main
			}
			sucObjCnt++
		}
		sucObjCnt++
	}
}
