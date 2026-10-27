package nats

import (
	"encoding/json"
	"testing"

	natsclient "github.com/nats-io/nats.go"
)

func TestRequestExtensionsRoundTrip(t *testing.T) {
	original := &Request{
		Prompt: "investigate device",
		Attachments: []Attachment{
			{Filename: "context.txt", Content: []byte("test attachment")},
		},
		Header: natsclient.Header{
			"Nicco-Run-Id": {"run-123"},
		},
		Extensions: map[string]json.RawMessage{
			"x_nicco":     json.RawMessage(`{"execution_preferences":{"version":"execution-preferences.v1","reasoning":"off"}}`),
			"prompt":      json.RawMessage(`"overridden"`),
			"attachments": json.RawMessage(`[]`),
		},
	}

	payload, err := json.Marshal(original.envelope())
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	decoded, err := decodeEnvelope(payload)
	if err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	received, err := requestFromEnvelope(decoded, original.Header)
	if err != nil {
		t.Fatalf("restore request: %v", err)
	}

	if received.Prompt != original.Prompt {
		t.Fatalf("prompt changed: %q", received.Prompt)
	}
	if len(received.Attachments) != 1 ||
		string(received.Attachments[0].Content) != "test attachment" {
		t.Fatalf("attachments changed: %#v", received.Attachments)
	}
	if received.Header.Get("Nicco-Run-Id") != "run-123" {
		t.Fatalf("headers changed: %#v", received.Header)
	}
	if _, exists := received.Extensions["prompt"]; exists {
		t.Fatal("reserved prompt field leaked into extensions")
	}
	if _, exists := received.Extensions["attachments"]; exists {
		t.Fatal("reserved attachments field leaked into extensions")
	}

	var extension struct {
		ExecutionPreferences struct {
			Reasoning string `json:"reasoning"`
		} `json:"execution_preferences"`
	}
	if err := json.Unmarshal(received.Extensions["x_nicco"], &extension); err != nil {
		t.Fatalf("decode x_nicco: %v", err)
	}
	if extension.ExecutionPreferences.Reasoning != "off" {
		t.Fatalf("x_nicco lost: %#v", extension)
	}
}

func TestRequestExtensionsInvalidJSON(t *testing.T) {
	request := &Request{
		Prompt: "test",
		Extensions: map[string]json.RawMessage{
			"x_invalid": json.RawMessage(`{broken`),
		},
	}
	if _, err := json.Marshal(request.envelope()); err == nil {
		t.Fatal("expected invalid extension JSON to be rejected")
	}
}

func TestRequestExtensionsIsolation(t *testing.T) {
	raw := json.RawMessage(`{"value":"original"}`)
	request := &Request{
		Prompt: "test",
		Extensions: map[string]json.RawMessage{
			"x_custom": raw,
		},
	}

	env := request.envelope()
	raw[0] = '['
	request.Extensions["x_custom"][1] = 'X'

	if got := string(env.Extensions["x_custom"]); got != `{"value":"original"}` {
		t.Fatalf("extension was not copied: %q", got)
	}
}
