package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// 1st elements is offset, 2nd is the message
type offsetAndMessage [2]int

type topic struct {
	mu       *sync.Mutex
	messages []int
}

func (t *topic) offset() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.messages) - 1
}

func (t *topic) addMsg(msg int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.messages = append(t.messages, msg)
}

func (t *topic) messagesFromOffset(offset int) []offsetAndMessage {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.messages) <= offset {
		return nil
	}

	var result []offsetAndMessage

	for i, v := range t.messages[offset:] {
		result = append(result, offsetAndMessage{i, v})
	}

	return result
}

type state struct {
	mu     sync.Mutex
	topics map[string]*topic
}

func (s *state) startLog(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics[key] = &topic{
		mu: &sync.Mutex{},
	}
}

func (s *state) topicOffset(key string) int {
	return s.topics[key].offset()
}

func (s *state) addMsg(key string, msg int) int {
	s.topics[key].addMsg(msg)
	return s.topics[key].offset()
}

func (s *state) exist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.topics[key]

	return ok
}

func newState() *state {
	return &state{
		mu:     sync.Mutex{},
		topics: make(map[string]*topic),
	}
}

func main() {
	s := newState()

	n := maelstrom.NewNode()

	n.Handle("send", func(msg maelstrom.Message) error {
		var req map[string]any
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		k, _ := req["key"]
		m, _ := req["msg"]

		keyFromReq := k.(string)
		msgFromReq := int(m.(float64))

		if !s.exist(keyFromReq) {
			s.startLog(keyFromReq)
		}

		offset := s.addMsg(keyFromReq, msgFromReq)

		resp := map[string]any{
			"type":   "send_ok",
			"offset": offset,
		}

		return n.Reply(msg, resp)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var req map[string]any
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		o, _ := req["offsets"]

		offsets, _ := o.(map[string]any)

		result := make(map[string][]offsetAndMessage)

		for k, v := range offsets {
			if !s.exist(k) {
				continue
			}

			t := s.topics[k]

			result[k] = t.messagesFromOffset(int(v.(float64)))
		}

		resp := map[string]any{
			"type": "poll_ok",
			"msgs": result,
		}

		return n.Reply(msg, resp)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var req map[string]any
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		resp := map[string]any{
			"type": "commit_offsets_ok",
		}

		return n.Reply(msg, resp)
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var req map[string]any
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		resp := map[string]any{
			"type": "list_committed_offsets_ok",
		}

		return n.Reply(msg, resp)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
