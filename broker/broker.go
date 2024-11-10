// broker/broker.go
package broker

import (
	"encoding/gob"
	"log"
	"net"
	"sync"

	"github.com/Distributed-Systems-KP/Pub-Sub-System/protocol"
	"github.com/Distributed-Systems-KP/Pub-Sub-System/storage"
)

type Broker struct {
	ID           string
	Address      string
	storage      *storage.Storage
	topics       map[string][]net.Conn // Topic to subscriber connections
	partitions   map[string][]int      // Topic to partition IDs
	partitionMap map[int][]net.Conn    // Partition ID to subscriber connections
	peers        map[string]string     // BrokerID to Address
	isLeader     bool
	cluster      *Cluster
	mu           sync.RWMutex
}

func NewBroker(id, address, dbPath string, isLeader bool) (*Broker, error) {
	store, err := storage.NewStorage(dbPath)
	if err != nil {
		return nil, err
	}

	broker := &Broker{
		ID:           id,
		Address:      address,
		storage:      store,
		topics:       make(map[string][]net.Conn),
		partitions:   make(map[string][]int),
		partitionMap: make(map[int][]net.Conn),
		peers:        make(map[string]string),
		isLeader:     isLeader,
	}

	broker.cluster = NewCluster(broker) // Initialize the cluster
	return broker, nil
}

func (b *Broker) Start() error {
	ln, err := net.Listen("tcp", b.Address)
	if err != nil {
		return err
	}
	defer ln.Close()
	log.Printf("Broker %s is running on %s...\n", b.ID, b.Address)

	go b.cluster.StartCluster() // Run cluster initialization in a separate goroutine

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}
		go b.handleConnection(conn)
	}
}

func (b *Broker) handleConnection(conn net.Conn) {
	decoder := gob.NewDecoder(conn)
	encoder := gob.NewEncoder(conn)

	for {
		var msg protocol.Message
		err := decoder.Decode(&msg)
		if err != nil {
			log.Println("Error decoding message:", err)
			conn.Close()
			return
		}

		switch msg.Type {
		case protocol.MsgPublish:
			b.handlePublish(msg, encoder)
		case protocol.MsgSubscribe:
			b.handleSubscribe(msg, conn)
		case protocol.MsgBrokerJoin:
			if b.isLeader {
				b.cluster.handleBrokerJoin(msg, encoder)
			} else {
				log.Println("Received broker join request, but not the leader.")
			}
		case protocol.MsgPartitionAssign:
			b.cluster.handlePartitionAssign(msg)
		default:
			log.Println("Unknown message type")
		}
	}
}

func (b *Broker) handlePublish(msg protocol.Message, encoder *gob.Encoder) {
	b.mu.RLock()
	subscribers, exists := b.partitionMap[msg.Partition]
	b.mu.RUnlock()
	if exists {
		for _, sub := range subscribers {
			subEncoder := gob.NewEncoder(sub)
			err := subEncoder.Encode(msg)
			if err != nil {
				log.Println("Error sending message to subscriber:", err)
			}
		}
	} else {
		log.Printf("No subscribers for partition %d\n", msg.Partition)
	}

	// Acknowledge the producer
	ack := protocol.Message{Type: protocol.MsgAcknowledge}
	if err := encoder.Encode(ack); err != nil {
		log.Println("Error sending acknowledgment:", err)
	}
}

func (b *Broker) handleSubscribe(msg protocol.Message, conn net.Conn) {
	b.mu.Lock()
	b.partitionMap[msg.Partition] = append(b.partitionMap[msg.Partition], conn)
	b.mu.Unlock()
	log.Printf("New subscriber for topic: %s, partition: %d\n", msg.Topic, msg.Partition)
}

func (b *Broker) Close() {
	b.storage.Close()
}
