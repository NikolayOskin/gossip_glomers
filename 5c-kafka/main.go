package main

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	mu    sync.Mutex
	casMu sync.Mutex

	node *maelstrom.Node
	kv   *maelstrom.KV

	logs             map[string][]entry
	committedOffsets map[string]int

	ch chan task
}

func newServer(node *maelstrom.Node, kv *maelstrom.KV) *server {
	return &server{
		mu:               sync.Mutex{},
		casMu:            sync.Mutex{},
		node:             node,
		kv:               kv,
		committedOffsets: make(map[string]int),
		logs:             make(map[string][]entry),
		ch:               make(chan task, 5000), // TODO: remove buffer
	}
}

type entry struct {
	offset  int
	message int
}

type task struct {
	key   string
	entry entry
}

func main() {
	node := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(node)

	s := newServer(node, kv)

	s.node.Handle("init", s.initHandler)
	s.node.Handle("send", s.sendHandler)
	s.node.Handle("poll", s.pollHandler)
	s.node.Handle("commit_offsets", s.commitOffsetsHandler)
	s.node.Handle("list_committed_offsets", s.listCommittedOffsetsHandler)
	s.node.Handle("sync", s.syncHandler)

	if err := s.node.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) initHandler(_ maelstrom.Message) error {
	go func() {
		s.worker()
	}()

	return nil
}

func (s *server) syncHandler(msg maelstrom.Message) error {
	var req map[string]any
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	key := req["key"].(string)
	offset := int(req["offset"].(float64))
	message := int(req["msg"].(float64))

	s.mu.Lock()
	s.logs[key] = append(s.logs[key], entry{
		offset:  offset,
		message: message,
	})
	s.mu.Unlock()

	resp := map[string]any{
		"type": "sync_ok",
	}

	return s.node.Reply(msg, resp)
}

func (s *server) sendHandler(msg maelstrom.Message) error {
	var req map[string]any
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	key := req["key"].(string)
	message := int(req["msg"].(float64))

	s.casMu.Lock()
	offset, _ := s.kv.ReadInt(context.Background(), key)
	newOffset := offset + 1

	if err := s.kv.CompareAndSwap(context.Background(), key, offset, newOffset, true); err != nil {
		return err
	}
	s.casMu.Unlock()

	s.mu.Lock()
	s.logs[key] = append(s.logs[key], entry{
		offset:  newOffset,
		message: message,
	})
	s.mu.Unlock()

	s.ch <- task{
		key: key,
		entry: entry{
			offset:  newOffset,
			message: message,
		},
	}

	resp := map[string]any{
		"type":   "send_ok",
		"offset": newOffset,
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

	for k, offset := range offsets {
		s.mu.Lock()
		entries, ok := s.logs[k]
		s.mu.Unlock()
		if !ok {
			continue
		}
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].offset < entries[j].offset
		})
		for _, e := range entries {
			if e.offset >= int(offset.(float64)) {
				result[k] = append(result[k], [2]int{e.offset, e.message})
			}
		}
	}

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

	for k, v := range offsets {
		s.committedOffsets[k] = int(v.(float64))
	}

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

	for _, key := range keys {
		k := key.(string)
		committedOffset, ok := s.committedOffsets[k]
		if ok {
			offsets[k] = committedOffset
		}
	}

	resp := map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets,
	}

	return s.node.Reply(msg, resp)
}

func (s *server) worker() {
	for v := range s.ch {
		for _, nodeID := range s.node.NodeIDs() {
			if nodeID == s.node.ID() {
				continue
			}
			_, _ = s.node.SyncRPC(
				context.Background(),
				nodeID,
				map[string]any{
					"type":   "sync",
					"key":    v.key,
					"offset": v.entry.offset,
					"msg":    v.entry.message,
				},
			)
		}
	}
}
