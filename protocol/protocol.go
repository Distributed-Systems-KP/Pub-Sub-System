// protocol/protocol.go
package protocol

// MessageType defines the type of the message sent between client and broker
type MessageType int

const (
	MsgPublish MessageType = iota
	MsgSubscribe
	MsgAcknowledge
	MsgBrokerJoin
	MsgBrokerInfo
	MsgPartitionAssign
)

// Message represents the structure of messages exchanged
type Message struct {
	Type       MessageType
	Topic      string
	Payload    string
	Partition  int
	BrokerID   string
	BrokerAddr string
	Brokers    map[string]string // Added this field
	Partitions map[string][]int
}
