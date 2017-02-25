package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"net"
	"regexp"
	"strings"
)

func InterpretConnectionString(cs string) (tls bool, url string) {
	const argsDelim string = "?"
	url = cs
	url_parts := strings.SplitN(cs, argsDelim, 2)
	if len(url_parts) == 2 {
		r := regexp.MustCompile(`ssl=true\&?`)
		repl := r.ReplaceAllString(url_parts[1], "")
		if repl != url_parts[1] {
			// ssl detected
			tls = true
			url = strings.Join([]string{url_parts[0], repl}, argsDelim)
		}
	}
	return
}

func DialMongo(ssl bool, url string) (*mgo.Session, error) {
	if ssl {
		tlsConfig := &tls.Config{}
		dialInfo, err := mgo.ParseURL(url)
		if err != nil {
			return nil, err
		}
		dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
			if err != nil {
				log.Printf("Unable to dial mongodb: %s", err)
			}
			return conn, err
		}
		return mgo.DialWithInfo(dialInfo)
	} else {
		return mgo.Dial(url)
	}
}

func main() {
	var cs string
	oplog_col := "oplog.$main"
	log.SetPrefix("ERROR: ")
	flag.StringVar(&cs, "url", "", "MongoDB connection URL")
	flag.Parse()
	if cs == "" {
		log.Fatalln("url is required")
	}
	ssl, url := InterpretConnectionString(cs)
	session, err := DialMongo(ssl, url)
	if err != nil {
		log.Fatalf("unable to connect...%s", err)
	}
	localDB := session.DB("local")
	col_names, err := localDB.CollectionNames()
	if err != nil {
		log.Fatalf("unable get collections in db local: %s", err)
	}
	fmt.Println("found the following collections in db local")
	for _, col := range col_names {
		fmt.Println("\t" + col)
		if strings.HasPrefix(col, "oplog.") {
			oplog_col = col
		}
	}
	query := bson.M{"ts": bson.M{"$gt": bson.MongoTimestamp(0)}, "fromMigrate": bson.M{"$exists": false}}
	queryJson, err := json.Marshal(query)
	if err != nil {
		log.Fatalf("unable to marshal json: %s", err)
	}
	fmt.Println(fmt.Sprintf("running query %s on collection local.%s", queryJson, oplog_col))
	collection := session.DB("local").C(oplog_col)
	var results []map[string]interface{}
	err = collection.Find(query).Limit(5).All(&results)
	if err != nil {
		log.Fatalf("unable to query local.%s: %s", oplog_col, err)
	}
	if len(results) == 0 {
		fmt.Println("nothing of iterest in the oplog")
	} else {
		fmt.Println("printing some ops from the oplog")
		for _, r := range results {
			j, err := json.Marshal(r)
			if err != nil {
				log.Fatalf("unable to marshal to json: %s", err)
			}
			fmt.Println(fmt.Sprintf("\t%s", j))
		}
	}
	fmt.Println("all done")
}
