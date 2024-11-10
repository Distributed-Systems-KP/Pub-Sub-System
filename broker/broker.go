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
	topics       map[string][]net.Conn
	partitions   map[string][]int
	partitionMap map[int][]*Subscriber
	peers        map[string]string
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
		partitionMap: make(map[int][]*Subscriber),
		peers:        make(map[string]string),
		isLeader:     isLeader,
	}

	broker.cluster = NewCluster(broker)
	return broker, nil
}

func (b *Broker) Start() error {
	ln, err := net.Listen("tcp", b.Address)
	if err != nil {
		return err
	}
	defer ln.Close()
	log.Printf("Broker %s is running on %s...\n", b.ID, b.Address)

	go b.cluster.StartCluster()

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
			err := sub.Encoder.Encode(msg)
			if err != nil {
				log.Println("Error sending message to subscriber:", err)
				b.removeSubscriber(msg.Partition, sub)
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
	subscriber := &Subscriber{
		Conn:    conn,
		Encoder: gob.NewEncoder(conn),
	}
	b.mu.Lock()
	b.partitionMap[msg.Partition] = append(b.partitionMap[msg.Partition], subscriber)
	b.mu.Unlock()
	log.Printf("New subscriber for topic: %s, partition: %d\n", msg.Topic, msg.Partition)
}

func (b *Broker) removeSubscriber(partition int, sub *Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	subscribers := b.partitionMap[partition]
	for i, s := range subscribers {
		if s == sub {
			s.Conn.Close()
			b.partitionMap[partition] = append(subscribers[:i], subscribers[i+1:]...)
			break
		}
	}
}

func (b *Broker) Close() {
	b.storage.Close()
}
