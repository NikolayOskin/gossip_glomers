package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type neighbor struct {
	messagesToSend []int
	id             string
}

type nodeState struct {
	mu            sync.Mutex
	neighborNodes map[string][]int
	messages      map[int]struct{}
	updatedAt     time.Time
}

func (ns *nodeState) addNeighborNodeID(nodeID string) {
	ns.neighborNodes[nodeID] = make([]int, 0)
}

func (ns *nodeState) addMessage(message int) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.messages[message] = struct{}{}
}

func (ns *nodeState) messagesList() []int {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	messages := make([]int, 0, len(ns.messages))
	for k, _ := range ns.messages {
		messages = append(messages, k)
	}

	sort.Ints(messages)

	return messages
}

func (ns *nodeState) addMessageToSend(nodeID string, message int) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.neighborNodes[nodeID] = append(ns.neighborNodes[nodeID], message)
}

func newNodeState() *nodeState {
	return &nodeState{
		messages:      make(map[int]struct{}),
		neighborNodes: make(map[string][]int),
	}
}

type nMessage struct {
	message int
	nodeID  string
}

func main() {
	node := newNodeState()
	n := maelstrom.NewNode()

	ch := make(chan nMessage, 1000)

	go func() {
		for c := range ch {
			ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*100)
			_, err := n.SyncRPC(ctx, c.nodeID, map[string]any{
				"type":    "sync",
				"message": c.message,
			})
			if err != nil {
				ch <- c
			}
		}
	}()

	n.Handle("topology", func(msg maelstrom.Message) error {
		var reqBody map[string]any
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			return err
		}

		topologyField, ok := reqBody["topology"]
		if !ok {
			return fmt.Errorf("topology is not set")
		}

		topology, ok := topologyField.(map[string]any)
		if !ok {
			return fmt.Errorf("topologyField has wrong type %v", topologyField)
		}

		for nodeID, _ := range topology {
			if nodeID != n.ID() {
				node.addNeighborNodeID(nodeID)
			}
		}

		return n.Reply(msg, map[string]string{
			"type": "topology_ok",
		})
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var reqBody map[string]any
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			return err
		}

		message, ok := reqBody["message"]
		if !ok {
			return errors.New("message is not set")
		}
		messageNum, ok := message.(float64)
		if !ok {
			return errors.New("message type invalid")
		}

		node.addMessage(int(messageNum))

		for nNode, _ := range node.neighborNodes {
			syncRequestBody := make(map[string]any)
			syncRequestBody["type"] = "sync"
			syncRequestBody["message"] = messageNum

			ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*300)

			_, err := n.SyncRPC(ctx, nNode, syncRequestBody)
			if err != nil {
				ch <- nMessage{
					nodeID:  nNode,
					message: int(messageNum),
				}
			}
		}

		return n.Reply(msg, map[string]string{
			"type": "broadcast_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var reqBody map[string]any
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": node.messagesList(),
		})
	})

	n.Handle("sync", func(msg maelstrom.Message) error {
		var reqBody map[string]any
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			return err
		}

		message, ok := reqBody["message"]
		if !ok {
			return errors.New("message is not set")
		}
		messageNum, ok := message.(float64)
		if !ok {
			return errors.New("message type invalid")
		}

		node.addMessage(int(messageNum))

		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
