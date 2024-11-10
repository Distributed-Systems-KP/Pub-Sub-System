package broker

import (
	"encoding/gob"
	"log"
	"net"
	"sync"
	"time"

	"github.com/Distributed-Systems-KP/Pub-Sub-System/protocol"
)

type Cluster struct {
	broker     *Broker
	leaderID   string
	leaderAddr string
	mu         sync.Mutex
}

func NewCluster(broker *Broker) *Cluster {
	return &Cluster{
		broker: broker,
	}
}

func (c *Cluster) StartCluster() {
	if c.broker.isLeader {
		c.startLeader()
	} else {
		c.joinCluster()
	}
}

func (c *Cluster) startLeader() {
	c.leaderID = c.broker.ID
	c.leaderAddr = c.broker.Address
	c.broker.peers[c.leaderID] = c.leaderAddr
	c.broker.storage.AddBroker(c.leaderID, c.leaderAddr)
	log.Printf("Broker %s is acting as the cluster leader.", c.leaderID)
}

func (c *Cluster) joinCluster() {
	c.discoverLeader()
	if c.leaderAddr == "" {
		log.Println("Leader not found. Retrying...")
		time.Sleep(5 * time.Second)
		c.joinCluster()
		return
	}

	conn, err := net.Dial("tcp", c.leaderAddr)
	if err != nil {
		log.Printf("Error connecting to leader at %s: %v", c.leaderAddr, err)
		time.Sleep(5 * time.Second)
		c.joinCluster()
		return
	}
	defer conn.Close()

	encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)

	// Send join request
	msg := protocol.Message{
		Type:       protocol.MsgBrokerJoin,
		BrokerID:   c.broker.ID,
		BrokerAddr: c.broker.Address,
	}
	err = encoder.Encode(msg)
	if err != nil {
		log.Printf("Error sending join request: %v", err)
		return
	}

	// Receive cluster info
	var response protocol.Message
	err = decoder.Decode(&response)
	if err != nil {
		log.Printf("Error receiving response from leader: %v", err)
		return
	}

	if response.Type == protocol.MsgBrokerInfo {
		c.mu.Lock()
		c.broker.peers = response.Brokers
		c.broker.partitions = response.Partitions
		c.mu.Unlock()
		log.Printf("Broker %s joined the cluster.", c.broker.ID)
	}
}

func (c *Cluster) discoverLeader() {
	c.leaderID = "broker1"
	c.leaderAddr = "localhost:8081"
}

func (c *Cluster) handleBrokerJoin(msg protocol.Message, encoder *gob.Encoder) {
	c.mu.Lock()
	c.broker.peers[msg.BrokerID] = msg.BrokerAddr
	c.broker.storage.AddBroker(msg.BrokerID, msg.BrokerAddr)
	c.mu.Unlock()
	log.Printf("Broker %s joined the cluster.", msg.BrokerID)

	// Send current cluster info to the joining broker
	brokers, _ := c.broker.storage.GetBrokers()
	partitions := make(map[string][]int)
	topics, _ := c.broker.storage.GetTopics()
	for _, topic := range topics {
		parts, _ := c.broker.storage.GetPartitions(topic)
		partitions[topic] = parts
	}

	response := protocol.Message{
		Type:       protocol.MsgBrokerInfo,
		BrokerID:   c.broker.ID,
		BrokerAddr: c.broker.Address,
		Brokers:    brokers,
		Partitions: partitions,
	}
	encoder.Encode(response)
}

func (c *Cluster) distributePartitions() {
	c.mu.Lock()
	defer c.mu.Unlock()

	topics, _ := c.broker.storage.GetTopics()
	for _, topic := range topics {
		numPartitions := 3 // For example
		brokers := c.broker.peers
		i := 0
		for partition := 0; partition < numPartitions; partition++ {
			brokerIDs := make([]string, 0, len(brokers))
			for id := range brokers {
				brokerIDs = append(brokerIDs, id)
			}
			assignedBrokerID := brokerIDs[i%len(brokerIDs)]
			c.broker.storage.AddPartition(topic, assignedBrokerID)
			c.broker.partitions[topic] = append(c.broker.partitions[topic], partition)
			i++
		}
	}
	// Send partition assignments to brokers
	for brokerID, brokerAddr := range c.broker.peers {
		if brokerID == c.broker.ID {
			continue // Skip leader
		}
		c.sendPartitionAssignment(brokerID, brokerAddr)
	}
}

func (c *Cluster) sendPartitionAssignment(brokerID, brokerAddr string) {
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		log.Printf("Error connecting to broker %s at %s: %v", brokerID, brokerAddr, err)
		return
	}
	defer conn.Close()

	encoder := gob.NewEncoder(conn)

	msg := protocol.Message{
		Type:       protocol.MsgPartitionAssign,
		BrokerID:   c.broker.ID,
		Partitions: c.broker.partitions,
	}
	err = encoder.Encode(msg)
	if err != nil {
		log.Printf("Error sending partition assignment to broker %s: %v", brokerID, err)
	}
}

func (c *Cluster) handlePartitionAssign(msg protocol.Message) {
	c.mu.Lock()
	c.broker.partitions = msg.Partitions
	c.mu.Unlock()
	log.Printf("Received partition assignments from leader.")
}
