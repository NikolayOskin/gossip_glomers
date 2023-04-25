package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)

	node.Handle("read", func(msg maelstrom.Message) error {
		var reqBody map[string]any
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			return err
		}

		u, err := uuid.NewUUID()
		if err != nil {
			return fmt.Errorf("uuid.NewUUID: %w", err)
		}

		// writing any unique value to be sure that later reads will not get stale results
		if err := kv.Write(context.Background(), "uuid", u.String()); err != nil {
			return fmt.Errorf("kv.Write: %w", err)
		}

		var gCounter int

		for _, nodeID := range node.NodeIDs() {
			ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*100)
			nodeCounter, err := kv.ReadInt(ctx, nodeID)
			if err != nil && maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
				return fmt.Errorf("ReadInt: %w", err)
			}
			gCounter = gCounter + nodeCounter
		}

		return node.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": gCounter,
		})
	})

	node.Handle("add", func(msg maelstrom.Message) error {
		var reqBody map[string]any
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			return err
		}

		delta, ok := reqBody["delta"]
		if !ok {
			return errors.New("delta is not set")
		}
		deltaNum, ok := delta.(float64)
		if !ok {
			return errors.New("message type invalid")
		}

		if deltaNum <= 0 {
			return node.Reply(msg, map[string]any{
				"type": "add_ok",
			})
		}

		ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*100)
		nodeCounter, err := kv.ReadInt(ctx, node.ID())
		if err != nil && maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
			return fmt.Errorf("ReadInt: %w", err)
		}

		ctx, _ = context.WithTimeout(context.Background(), time.Millisecond*100)
		err = kv.CompareAndSwap(
			ctx,
			node.ID(),
			nodeCounter,
			nodeCounter+int(deltaNum),
			true,
		)
		if err != nil {
			return fmt.Errorf("CompareAndSwap: %w", err)
		}

		return node.Reply(msg, map[string]any{
			"type": "add_ok",
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
