// client/producer/main.go
package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/Distributed-Systems-KP/Pub-Sub-System/protocol"
)

func main() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter broker address (e.g., localhost:8080): ")
	brokerAddr, _ := reader.ReadString('\n')
	brokerAddr = strings.TrimSpace(brokerAddr)

	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		fmt.Println("Error connecting to broker:", err)
		return
	}
	defer conn.Close()

	encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)

	fmt.Print("Enter topic: ")
	topic, _ := reader.ReadString('\n')
	topic = strings.TrimSpace(topic)

	fmt.Print("Enter partition (integer): ")
	partitionStr, _ := reader.ReadString('\n')
	partitionStr = strings.TrimSpace(partitionStr)
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		fmt.Println("Invalid partition number:", err)
		return
	}

	for {
		fmt.Print("Enter message (or 'quit' to exit): ")
		payload, _ := reader.ReadString('\n')
		payload = strings.TrimSpace(payload)

		if payload == "quit" {
			break
		}

		msg := protocol.Message{
			Type:      protocol.MsgPublish,
			Topic:     topic,
			Payload:   payload,
			Partition: partition,
		}

		err = encoder.Encode(msg)
		if err != nil {
			fmt.Println("Error sending message:", err)
			continue
		}

		// Wait for acknowledgment
		var ack protocol.Message
		err = decoder.Decode(&ack)
		if err != nil {
			fmt.Println("Error receiving acknowledgment:", err)
			continue
		}
		if ack.Type == protocol.MsgAcknowledge {
			fmt.Println("Message published successfully.")
		}
	}
}
