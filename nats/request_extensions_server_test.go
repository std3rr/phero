package nats

import (
	"encoding/json"
	"testing"
	"time"

	natsio "github.com/nats-io/nats.go/micro"
)

func TestProcessPromptPreservesApplicationExtensions(t *testing.T) {
	body := []byte(`{
		"prompt": "investigate circuit",
		"x_nicco": {
			"execution_preferences": {
				"version": "execution-preferences.v1",
				"reasoning": "off"
			},
			"conversation": {
				"history": [
					{"role": "user", "content": "previous question"}
				]
			}
		}
	}`)

	s, ctx := newTestServer(t, time.Second)

	handler := &recordingHandler{}
	s.handler = handler

	headers := natsio.Headers{
		"Nicco-Run-Id": []string{"run-123"},
	}

	req := &fakeRequest{
		data:    body,
		headers: headers,
	}

	s.processPrompt(ctx, req)

	if req.errCode != "" {
		t.Fatalf("NAP request rejected: %s %s", req.errCode, req.errDesc)
	}
	if handler.got == nil {
		t.Fatal("handler did not receive RequestFrom context")
	}
	if handler.got.Prompt != "investigate circuit" {
		t.Fatalf("prompt = %q", handler.got.Prompt)
	}
	if handler.got.Header.Get("Nicco-Run-Id") != "run-123" {
		t.Fatalf("run ID header lost: %#v", handler.got.Header)
	}

	raw, ok := handler.got.Extensions["x_nicco"]
	if !ok {
		t.Fatal("x_nicco extension lost at server boundary")
	}

	var extension struct {
		ExecutionPreferences struct {
			Version   string `json:"version"`
			Reasoning string `json:"reasoning"`
		} `json:"execution_preferences"`
		Conversation struct {
			History []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"history"`
		} `json:"conversation"`
	}
	if err := json.Unmarshal(raw, &extension); err != nil {
		t.Fatalf("decode x_nicco: %v", err)
	}

	if extension.ExecutionPreferences.Version != "execution-preferences.v1" ||
		extension.ExecutionPreferences.Reasoning != "off" {
		t.Fatalf("execution preferences lost: %#v", extension)
	}
	if len(extension.Conversation.History) != 1 ||
		extension.Conversation.History[0].Content != "previous question" {
		t.Fatalf("conversation history lost: %#v", extension.Conversation)
	}

	if _, ok := handler.got.Extensions["prompt"]; ok {
		t.Fatal("protocol prompt leaked into application extensions")
	}
	if _, ok := handler.got.Extensions["attachments"]; ok {
		t.Fatal("protocol attachments leaked into application extensions")
	}

	if len(req.responses) == 0 {
		t.Fatal("server produced no NAP response")
	}

}
