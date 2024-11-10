// broker/subscriber.go
package broker

import (
	"encoding/gob"
	"net"
)

// Subscriber represents a subscriber's connection and encoder.
type Subscriber struct {
	Conn    net.Conn
	Encoder *gob.Encoder
}
