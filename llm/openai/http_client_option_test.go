package openai

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/henomis/phero/v2/llm"
)

type headerInjectingTransport struct {
	base http.RoundTripper
}

func (t headerInjectingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	cloned := req.Clone(req.Context())
	cloned.Header.Set("X-Phero-HTTP-Client-Test", "injected")
	return t.base.RoundTrip(cloned)
}

func TestWithHTTPClientUsesInjectedTransport(t *testing.T) {
	var received bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = true

		if got := r.Header.Get("X-Phero-HTTP-Client-Test"); got != "injected" {
			t.Errorf("injected header = %q, want injected", got)
		}

		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("request path = %q", r.URL.Path)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":      "chatcmpl-test",
			"object":  "chat.completion",
			"created": 1,
			"model":   "test-model",
			"choices": []any{
				map[string]any{
					"index": 0,
					"message": map[string]any{
						"role":    "assistant",
						"content": "transport verified",
					},
					"finish_reason": "stop",
				},
			},
		})
	}))
	defer server.Close()

	client := &http.Client{
		Transport: headerInjectingTransport{
			base: http.DefaultTransport,
		},
	}

	model := New(
		"test-key",
		WithBaseURL(server.URL+"/v1"),
		WithModel("test-model"),
		WithHTTPClient(client),
	)

	result, err := model.Execute(
		context.Background(),
		[]llm.Message{
			{
				Role:  llm.RoleUser,
				Parts: []llm.ContentPart{llm.Text("ping")},
			},
		},
	)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if !received {
		t.Fatal("test server did not receive a request")
	}

	if result == nil {
		t.Fatal("expected non-nil result")
	}
}
