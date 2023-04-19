package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type nodeState struct {
	mu            sync.Mutex
	neighborNodes []string
	messages      map[int]struct{}
}

func (ns *nodeState) addNeighborNode(node string) {
	ns.neighborNodes = append(ns.neighborNodes, node)
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

	return messages
}

func newNodeState() *nodeState {
	return &nodeState{
		messages: make(map[int]struct{}),
	}
}

func main() {
	node := newNodeState()
	n := maelstrom.NewNode()

	n.Handle("topology", func(msg maelstrom.Message) error {
		var reqBody map[string]any
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			return err
		}

		topology, ok := reqBody["topology"]
		if !ok {
			return fmt.Errorf("topology is not set")
		}

		topologyMap, ok := topology.(map[string]any)
		if !ok {
			return fmt.Errorf("topology has wrong type %v", topology)
		}

		for k, _ := range topologyMap {
			if k != n.ID() {
				node.addNeighborNode(k)
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

		for _, node := range node.neighborNodes {
			syncRequestBody := make(map[string]any)
			syncRequestBody["type"] = "sync"
			syncRequestBody["message"] = messageNum

			err := n.Send(node, syncRequestBody)
			if err != nil {
				return err
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
