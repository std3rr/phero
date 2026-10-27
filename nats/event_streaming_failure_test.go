package nats

import (
	"context"
	"errors"
	"iter"
	"strings"
	"testing"
	"time"

	"github.com/henomis/phero/v2/agent"
	"github.com/henomis/phero/v2/llm"
	natsio "github.com/nats-io/nats.go/micro"
)

type failingStreamHandler struct {
	fail      bool
	oversized bool
}

func (h *failingStreamHandler) Run(context.Context, ...llm.ContentPart) (*agent.Result, error) {
	return nil, errors.New("Run must not be invoked")
}

func (h *failingStreamHandler) RunStream(context.Context, ...llm.ContentPart) iter.Seq2[agent.Event, error] {
	return func(yield func(agent.Event, error) bool) {
		delta := "partial"
		if h.oversized {
			delta = strings.Repeat("x", natsDefaultMaxPayload+1)
		}
		if !yield(agent.Event{Type: agent.EventTextDelta, TextDelta: delta}, nil) {
			return
		}
		if h.fail {
			yield(agent.Event{}, errors.New("model stream failed"))
			return
		}
		yield(agent.Event{Type: agent.EventDone, Result: &agent.Result{
			Parts: []llm.ContentPart{llm.Text("complete")},
		}}, nil)
	}
}

func TestEventStreamingFailuresAreNotSuccessfulResponses(t *testing.T) {
	for _, tc := range []struct {
		name      string
		handler   failingStreamHandler
		wantError string
	}{
		{"stream error", failingStreamHandler{fail: true}, errCodeInternal},
		{"oversized event", failingStreamHandler{oversized: true}, errCodeResponseTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, ctx := newTestServer(t, time.Second)
			WithEventStreaming(true)(s.cfg)
			s.handler = &tc.handler

			req := &fakeRequest{
				data: []byte(`{"prompt":"test"}`),
				headers: natsio.Headers{
					"Phero-Response-Mode": []string{"events"},
				},
			}

			s.processPrompt(ctx, req)

			if req.errCode != "500" {
				t.Fatalf("error code = %q, want 500", req.errCode)
			}
			if !strings.Contains(string(req.errBody), tc.wantError) {
				t.Fatalf("error body = %s, want %q", req.errBody, tc.wantError)
			}
			if len(req.responses) == 0 || len(req.responses[len(req.responses)-1]) != 0 {
				t.Fatal("missing zero-byte stream terminator")
			}

			for _, chunk := range req.responses {
				if strings.Contains(string(chunk), `"type":"response"`) {
					t.Fatalf("unexpected successful final response: %s", chunk)
				}
			}
		})
	}
}
