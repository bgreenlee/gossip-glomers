package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	set "github.com/deckarep/golang-set/v2"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	messages := set.NewSet[int]() // messages we've received
	var neighbors []string        // our neighbor nodes from the topology message

	awaitingAcks := map[int][]string{} // map of message id -> nodes we're awaiting an ack from
	var awaitingAcksMutex sync.RWMutex

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

		message := broadcastMessage.Message
		messageId := broadcastMessage.MsgId

		// store and forward new messages
		if !messages.Contains(message) {
			messages.Add(message)

			// forward message to our neighbors
			for _, node := range neighbors {
				if node != msg.Src { // don't send it to our source
					awaitingAcksMutex.Lock()
					awaitingAcks[messageId] = append(awaitingAcks[messageId], node)
					awaitingAcksMutex.Unlock()
				}
			}

			// send anything in awaitingAcks, and keep retrying until everything has acked
			for len(awaitingAcks[messageId]) > 0 {
				awaitingAcksMutex.RLock()
				for _, node := range awaitingAcks[messageId] {
					n.Send(node, msg.Body)
				}
				awaitingAcksMutex.RUnlock()
				time.Sleep(1000 * time.Millisecond)
			}
		}

		return nil
	})

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		var broadcastOkMessage struct {
			Type      string
			InReplyTo int
		}
		if err := json.Unmarshal(msg.Body, &broadcastOkMessage); err != nil {
			return err
		}

		// remove from awaitingAcks
		awaitingAcksMutex.Lock()
		defer awaitingAcksMutex.Unlock()
		nodes := awaitingAcks[broadcastOkMessage.InReplyTo]
		if len(nodes) <= 1 {
			awaitingAcks[broadcastOkMessage.InReplyTo] = []string{}
		} else {
			// find our node
			var i int
			for i = 0; i < len(nodes) && nodes[i] != msg.Src; i++ {
			}
			// delete by replacing our node with the last one, and then truncating the slice by one
			nodes[i] = nodes[len(nodes)-1]
			awaitingAcks[broadcastOkMessage.InReplyTo] = nodes[:len(nodes)-1]
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
