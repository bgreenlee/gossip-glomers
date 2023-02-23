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

	awaitingAcks := map[int]string{} // map of message id -> node we're awaiting an ack from
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

		message := broadcastMessage.Message
		messageId := broadcastMessage.MsgId

		// ack immediately
		n.Reply(msg, map[string]any{
			"type":        "broadcast_ok",
			"in_reply_to": messageId,
		})

		// store and forward new messages
		if !messages.Contains(message) {
			messages.Add(message)

			// forward message to our neighbors
			for _, node := range neighbors {
				if node != msg.Src { // don't send it to our source
					awaitingAcksMutex.Lock()
					awaitingAcks[messageId] = node
					awaitingAcksMutex.Unlock()
				}
			}

			// send anything in awaitingAcks, and keep retrying until everything has acked
			go func() {
				for len(awaitingAcks) > 0 {
					awaitingAcksMutex.RLock()
					for _, node := range awaitingAcks {
						n.Send(node, msg.Body)
					}
					awaitingAcksMutex.RUnlock()
					time.Sleep(1000 * time.Millisecond)
				}
			}()
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
		delete(awaitingAcks, broadcastOkMessage.InReplyTo)
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
