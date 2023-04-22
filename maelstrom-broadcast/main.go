package main

import (
	"container/list"
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

const sendTimeout = time.Millisecond * 50

type nodeState struct {
	mu                     sync.Mutex
	neighborNodes          []string
	messages               map[int]struct{}
	neighborFailedMessages map[string]failedMessages
}

func (ns *nodeState) addNeighborNodeID(nodeID string) {
	ns.neighborNodes = append(ns.neighborNodes, nodeID)
}

func (ns *nodeState) addMessage(message int) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.messages[message] = struct{}{}
}

func (ns *nodeState) messagesList() []int {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	var l []int

	for k := range ns.messages {
		l = append(l, k)
	}

	sort.Ints(l)

	return l
}

type failedMessages struct {
	mu   *sync.Mutex
	list *list.List
}

func (fm *failedMessages) add(message int) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	fm.list.PushBack(message)
}

func (fm *failedMessages) first() (int, bool) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	elem := fm.list.Front()
	if elem == nil {
		return 0, false
	}

	msg, _ := elem.Value.(int)

	return msg, true
}

func (fm *failedMessages) moveFirstToBack() {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	e := fm.list.Front()
	if e != nil {
		fm.list.MoveToBack(e)
	}
}

func newNodeState() *nodeState {
	return &nodeState{
		neighborFailedMessages: make(map[string]failedMessages),
		messages:               make(map[int]struct{}),
	}
}

func main() {
	node := newNodeState()
	n := maelstrom.NewNode()

	workerFn := func() {
		for nodeID, nodeFailedMessages := range node.neighborFailedMessages {
			nodeMessages := nodeFailedMessages
			nID := nodeID
			go func() {
				for {
					msg, ok := nodeMessages.first()
					if !ok {
						// empty list
						time.Sleep(10 * time.Millisecond)
						continue
					}

					ctx, _ := context.WithTimeout(context.Background(), sendTimeout)
					_, err := n.SyncRPC(ctx, nID, map[string]any{
						"type":    "sync",
						"message": msg,
					})
					if err != nil {
						nodeMessages.moveFirstToBack()
					}
				}
			}()
		}
	}

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
				node.neighborFailedMessages[nodeID] = failedMessages{
					list: list.New(),
					mu:   &sync.Mutex{},
				}
			}
		}

		workerFn()

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

		for _, nNode := range node.neighborNodes {
			nNode := nNode
			go func() {
				syncRequestBody := map[string]any{
					"type":    "sync",
					"message": messageNum,
				}

				ctx, _ := context.WithTimeout(context.Background(), sendTimeout)
				_, err := n.SyncRPC(ctx, nNode, syncRequestBody)
				if err != nil {
					nodeFailedMessages, _ := node.neighborFailedMessages[nNode]
					nodeFailedMessages.add(int(messageNum))
				}
			}()
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
