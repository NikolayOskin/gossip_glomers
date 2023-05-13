package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	mu   sync.RWMutex
	node *maelstrom.Node

	logs             map[string][]entry
	latestOffsets    map[string]int
	committedOffsets map[string]int
}

func newServer(n *maelstrom.Node) *server {
	return &server{
		mu:   sync.RWMutex{},
		node: n,

		latestOffsets:    make(map[string]int),
		committedOffsets: make(map[string]int),
		logs:             make(map[string][]entry),
	}
}

type entry struct {
	offset  int
	message int
}

func main() {
	s := newServer(maelstrom.NewNode())

	s.node.Handle("send", s.sendHandler)
	s.node.Handle("poll", s.pollHandler)
	s.node.Handle("commit_offsets", s.commitOffsetsHandler)
	s.node.Handle("list_committed_offsets", s.listCommittedOffsetsHandler)

	if err := s.node.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) sendHandler(msg maelstrom.Message) error {
	var req map[string]any
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	key := req["key"].(string)
	message := int(req["msg"].(float64))

	s.mu.Lock()
	offset := s.latestOffsets[key] + 1
	s.logs[key] = append(s.logs[key], entry{
		offset:  offset,
		message: message,
	})
	s.latestOffsets[key] = offset
	s.mu.Unlock()

	resp := map[string]any{
		"type":   "send_ok",
		"offset": offset,
	}

	return s.node.Reply(msg, resp)
}

func (s *server) pollHandler(msg maelstrom.Message) error {
	var req map[string]any
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	offsets := req["offsets"].(map[string]any)

	result := make(map[string][][2]int)

	s.mu.Lock()
	for k, offset := range offsets {
		entries, ok := s.logs[k]
		if !ok {
			continue
		}

		for _, e := range entries {
			if e.offset >= int(offset.(float64)) {
				result[k] = append(result[k], [2]int{e.offset, e.message})
			}
		}
	}
	s.mu.Unlock()

	resp := map[string]any{
		"type": "poll_ok",
		"msgs": result,
	}

	return s.node.Reply(msg, resp)
}

func (s *server) commitOffsetsHandler(msg maelstrom.Message) error {
	var req map[string]any
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	offsets := req["offsets"].(map[string]any)

	s.mu.Lock()
	for k, v := range offsets {
		s.committedOffsets[k] = int(v.(float64))
	}
	s.mu.Unlock()

	resp := map[string]any{
		"type": "commit_offsets_ok",
	}

	return s.node.Reply(msg, resp)
}

func (s *server) listCommittedOffsetsHandler(msg maelstrom.Message) error {
	var req map[string]any
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	keys := req["keys"].([]any)
	offsets := make(map[string]int)

	s.mu.Lock()
	for _, key := range keys {
		k := key.(string)
		_, ok := s.committedOffsets[k]
		if ok {
			offsets[k] = s.committedOffsets[k]
		}
	}
	s.mu.Unlock()

	resp := map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets,
	}

	return s.node.Reply(msg, resp)
}
