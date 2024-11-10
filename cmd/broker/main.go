// broker/main.go
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/Distributed-Systems-KP/Pub-Sub-System/broker"
)

func main() {
	var (
		id       string
		address  string
		dbPath   string
		isLeader bool
	)

	flag.StringVar(&id, "id", "broker1", "Unique Broker ID")
	flag.StringVar(&address, "address", "localhost:8081", "Broker address")
	flag.StringVar(&dbPath, "db", "", "Database file path")
	flag.BoolVar(&isLeader, "leader", false, "Is this broker the cluster leader")
	flag.Parse()

	if dbPath == "" {
		dbPath = fmt.Sprintf("%s.db", id)
	}

	brokerInstance, err := broker.NewBroker(id, address, dbPath, isLeader)
	if err != nil {
		log.Fatal("Error initializing broker:", err)
	}
	defer brokerInstance.Close()

	err = brokerInstance.Start()
	if err != nil {
		log.Fatal("Error starting broker:", err)
	}
}
