package nats

import "testing"

func TestWithEndpointMetadata(t *testing.T) {
	input := map[string]string{
		"reasoning_preferences": "auto,off",
		"custom_feature":        "enabled",
		"max_payload":           "untrusted",
		"attachments_ok":        "untrusted",
	}

	opt := WithEndpointMetadata(input)

	// Mutating the caller's map after constructing the option
	// must not modify the server configuration.
	input["reasoning_preferences"] = "changed"
	input["custom_feature"] = "changed"

	var cfg serverConfig
	opt(&cfg)

	s := &Server{cfg: &cfg}

	metadata := s.promptMetadata()

	if got := metadata["reasoning_preferences"]; got != "auto,off" {
		t.Errorf("reasoning_preferences = %q, want auto,off", got)
	}
	if got := metadata["custom_feature"]; got != "enabled" {
		t.Errorf("custom_feature = %q, want enabled", got)
	}
	if got := metadata["max_payload"]; got == "untrusted" {
		t.Error("max_payload was overridden")
	}
	if got := metadata["attachments_ok"]; got == "untrusted" {
		t.Error("attachments_ok was overridden")
	}
}
