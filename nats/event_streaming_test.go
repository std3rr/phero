package nats

import (
	"bytes"
	"context"
	"encoding/json"
	"iter"
	"testing"
	"time"

	"github.com/henomis/phero/v2/agent"
	"github.com/henomis/phero/v2/llm"
	natsio "github.com/nats-io/nats.go/micro"
)

type testStreamingHandler struct {
	runCalls    int
	streamCalls int
}

func (h *testStreamingHandler) Run(
	_ context.Context,
	_ ...llm.ContentPart,
) (*agent.Result, error) {
	h.runCalls++
	return &agent.Result{
		Parts: []llm.ContentPart{llm.Text("buffered")},
	}, nil
}

func (h *testStreamingHandler) RunStream(
	_ context.Context,
	_ ...llm.ContentPart,
) iter.Seq2[agent.Event, error] {
	h.streamCalls++
	return func(yield func(agent.Event, error) bool) {
		if !yield(agent.Event{
			Type:      agent.EventTextDelta,
			TextDelta: "hello ",
		}, nil) {
			return
		}
		if !yield(agent.Event{
			Type:           agent.EventReasoningDelta,
			ReasoningDelta: "private reasoning",
		}, nil) {
			return
		}
		if !yield(agent.Event{
			Type:       agent.EventToolResult,
			ToolName:   "internal_tool",
			ToolResult: "private tool output",
		}, nil) {
			return
		}
		if !yield(agent.Event{
			Type:      agent.EventTextDelta,
			TextDelta: "world",
		}, nil) {
			return
		}
		yield(agent.Event{
			Type: agent.EventDone,
			Result: &agent.Result{
				Parts: []llm.ContentPart{llm.Text("hello world")},
			},
		}, nil)
	}
}

func TestEventStreamingOptIn(t *testing.T) {
	s, ctx := newTestServer(t, time.Second)
	h := &testStreamingHandler{}
	s.handler = h

	// New server option, intentionally not implemented yet.
	WithEventStreaming(true)(s.cfg)

	req := &fakeRequest{
		data: []byte(`{"prompt":"hello"}`),
		headers: natsio.Headers{
			"Phero-Response-Mode": []string{"events"},
		},
	}

	s.processPrompt(ctx, req)

	if h.runCalls != 0 || h.streamCalls != 1 {
		t.Fatalf("Run=%d RunStream=%d, want 0/1", h.runCalls, h.streamCalls)
	}

	var gotTypes []string
	var finalText string

	for _, raw := range req.responses {
		if len(raw) == 0 {
			continue
		}
		var chunk struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(raw, &chunk); err != nil {
			t.Fatal(err)
		}
		gotTypes = append(gotTypes, chunk.Type)

		switch chunk.Type {
		case "agent_event":
			var event struct {
				Event string `json:"event"`
				Delta string `json:"delta"`
			}
			if err := json.Unmarshal(chunk.Data, &event); err != nil {
				t.Fatal(err)
			}
			if event.Event != "text_delta" {
				t.Fatalf("unexpected exposed agent event: %q", event.Event)
			}
		case "response":
			if err := json.Unmarshal(chunk.Data, &finalText); err != nil {
				t.Fatal(err)
			}
		}
	}

	if finalText != "hello world" {
		t.Fatalf("final response = %q", finalText)
	}

	for _, response := range req.responses {
		if bytes.Contains(response, []byte("private reasoning")) ||
			bytes.Contains(response, []byte("private tool output")) {
			t.Fatal("sensitive event content leaked onto NATS")
		}
	}
	if len(gotTypes) != 4 ||
		gotTypes[0] != "status" ||
		gotTypes[1] != "agent_event" ||
		gotTypes[2] != "agent_event" ||
		gotTypes[3] != "response" {
		t.Fatalf("chunk types = %v", gotTypes)
	}
	if len(req.responses[len(req.responses)-1]) != 0 {
		t.Fatal("missing zero-byte terminator")
	}
}

func TestEventStreamingRequiresServerAndRequestOptIn(t *testing.T) {
	tests := []struct {
		name          string
		serverEnabled bool
		requestEvents bool
	}{
		{"server disabled", false, true},
		{"request did not opt in", true, false},
		{"neither opted in", false, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, ctx := newTestServer(t, time.Second)
			h := &testStreamingHandler{}
			s.handler = h
			WithEventStreaming(tc.serverEnabled)(s.cfg)

			req := &fakeRequest{
				data: []byte(`{"prompt":"hello"}`),
			}
			if tc.requestEvents {
				req.headers = natsio.Headers{
					"Phero-Response-Mode": []string{"events"},
				}
			}

			s.processPrompt(ctx, req)

			if h.runCalls != 1 || h.streamCalls != 0 {
				t.Fatalf("Run=%d RunStream=%d, want 1/0", h.runCalls, h.streamCalls)
			}
			if req.errCode != "" {
				t.Fatalf("request failed: %s %s", req.errCode, req.errDesc)
			}

			var responses []string
			for _, raw := range req.responses {
				if len(raw) == 0 {
					continue
				}
				var chunk struct {
					Type string `json:"type"`
				}
				if err := json.Unmarshal(raw, &chunk); err != nil {
					t.Fatal(err)
				}
				responses = append(responses, chunk.Type)
			}

			if len(responses) != 2 ||
				responses[0] != "status" ||
				responses[1] != "response" {
				t.Fatalf("legacy NAP response changed: %v", responses)
			}
		})
	}
}
