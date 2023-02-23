package main

import (
	"encoding/json"
	"log"
	"time"

	set "github.com/deckarep/golang-set/v2"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	messages := set.NewSet[int]() // messages we've received
	var neighbors []string        // our neighbor nodes from the topology message

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var broadcastMessage struct {
			Type    string
			Message int
			MsgId   int
		}
		if err := json.Unmarshal(msg.Body, &broadcastMessage); err != nil {
			return err
		}

		// ack immediately
		n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})

		awaitingAcks := set.NewSet[string]()
		message := broadcastMessage.Message

		// store and forward new messages
		if !messages.Contains(message) {
			messages.Add(message)

			// forward message to our neighbors
			for _, node := range neighbors {
				if node != msg.Src { // don't send it to our source
					awaitingAcks.Add(node)
				}
			}

			// send anything in awaitingAcks, and keep retrying until everything has acked
			for awaitingAcks.Cardinality() > 0 {
				for node := range awaitingAcks.Iter() {
					go n.RPC(node, msg.Body, func(msg maelstrom.Message) error {
						var broadcastOkMessage struct {
							Type      string
							InReplyTo int
						}
						if err := json.Unmarshal(msg.Body, &broadcastOkMessage); err != nil {
							return err
						}
						awaitingAcks.Remove(node)
						return nil
					})
				}
				time.Sleep(1000 * time.Millisecond)
			}
		}

		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": messages.ToSlice(),
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var topologyMessage struct {
			Type     string
			Topology map[string][]string
		}
		if err := json.Unmarshal(msg.Body, &topologyMessage); err != nil {
			return err
		}

		neighbors = topologyMessage.Topology[msg.Dest]

		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
