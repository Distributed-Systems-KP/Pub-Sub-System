// client/consumer/consumer.go
package main

import (
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/Distributed-Systems-KP/Pub-Sub-System/protocol"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: go run consumer.go <broker address> <topic> <partition>")
		return
	}
	brokerAddr := os.Args[1]
	topic := os.Args[2]
	partitionStr := os.Args[3]
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		fmt.Println("Invalid partition number:", err)
		return
	}

	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		fmt.Println("Error connecting to broker:", err)
		return
	}
	defer conn.Close()

	encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)

	// Send subscribe message
	msg := protocol.Message{
		Type:      protocol.MsgSubscribe,
		Topic:     topic,
		Partition: partition,
	}
	err = encoder.Encode(msg)
	if err != nil {
		fmt.Println("Error sending subscribe message:", err)
		return
	}

	fmt.Printf("Subscribed to topic: %s, partition: %d\n", topic, partition)
	for {
		var incomingMsg protocol.Message
		err = decoder.Decode(&incomingMsg)
		if err != nil {
			fmt.Println("Error receiving message:", err)
			return
		}

		if incomingMsg.Type == protocol.MsgPublish && incomingMsg.Topic == topic && incomingMsg.Partition == partition {
			fmt.Printf("Received message on topic '%s', partition '%d': %s\n", incomingMsg.Topic, incomingMsg.Partition, incomingMsg.Payload)
		}
	}
}
