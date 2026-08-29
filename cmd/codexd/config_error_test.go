package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/config"
)

func TestSafeConfigErrorAttributes(t *testing.T) {
	t.Run("reason without key", func(t *testing.T) {
		attrs := safeConfigErrorAttributes(&config.LoadError{Reason: "resolve home directory"})
		if want := []any{"reason", "resolve home directory"}; !equalAttributes(attrs, want) {
			t.Fatalf("safeConfigErrorAttributes() = %#v, want %#v", attrs, want)
		}
	})

	t.Run("key and reason", func(t *testing.T) {
		attrs := safeConfigErrorAttributes(&config.LoadError{
			Key:    "socket_path",
			Reason: "resolve home directory",
		})
		if want := []any{"key", "socket_path", "reason", "resolve home directory"}; !equalAttributes(attrs, want) {
			t.Fatalf("safeConfigErrorAttributes() = %#v, want %#v", attrs, want)
		}
	})

	t.Run("ordinary error", func(t *testing.T) {
		if attrs := safeConfigErrorAttributes(errors.New("x")); len(attrs) != 0 {
			t.Fatalf("safeConfigErrorAttributes() = %#v, want no attributes", attrs)
		}
	})

	t.Run("nil Err retains reason without cause type", func(t *testing.T) {
		// production非代表の防御的テスト: LoadError.Err が nil でも診断情報を安全に返す。
		attrs := safeConfigErrorAttributes(&config.LoadError{Reason: "resolve home directory"})
		if want := []any{"reason", "resolve home directory"}; !equalAttributes(attrs, want) {
			t.Fatalf("safeConfigErrorAttributes() = %#v, want %#v", attrs, want)
		}
	})

	t.Run("wrapped cause reports its type", func(t *testing.T) {
		cause := configErrorCause{}
		attrs := safeConfigErrorAttributes(&config.LoadError{
			Reason: "decode TOML configuration",
			Err:    fmt.Errorf("wrapped cause: %w", cause),
		})
		if want := []any{
			"reason", "decode TOML configuration",
			"cause_type", fmt.Sprintf("%T", cause),
		}; !equalAttributes(attrs, want) {
			t.Fatalf("safeConfigErrorAttributes() = %#v, want %#v", attrs, want)
		}
	})

	t.Run("does not disclose error text", func(t *testing.T) {
		err := &config.LoadError{
			Reason: "decode TOML configuration",
			Err:    errors.New("SENTINEL-xyz"),
		}
		output := fmt.Sprint(safeConfigErrorAttributes(err)) + safeConfigErrorMessage(err)
		if strings.Contains(output, "SENTINEL-xyz") {
			t.Fatalf("diagnostic output disclosed error text: %q", output)
		}
	})

	t.Run("empty reason is omitted", func(t *testing.T) {
		attrs := safeConfigErrorAttributes(&config.LoadError{Key: "socket_path"})
		if want := []any{"key", "socket_path"}; !equalAttributes(attrs, want) {
			t.Fatalf("safeConfigErrorAttributes() = %#v, want %#v", attrs, want)
		}
	})
}

func TestSafeConfigErrorMessage(t *testing.T) {
	t.Run("reason without key", func(t *testing.T) {
		message := safeConfigErrorMessage(&config.LoadError{Reason: "resolve home directory"})
		if !strings.Contains(message, "resolve home directory") {
			t.Fatalf("safeConfigErrorMessage() = %q, want reason", message)
		}
	})

	t.Run("key and reason", func(t *testing.T) {
		message := safeConfigErrorMessage(&config.LoadError{
			Key:    "socket_path",
			Reason: "resolve home directory",
		})
		if !strings.Contains(message, "configuration load failed for key socket_path") {
			t.Fatalf("safeConfigErrorMessage() = %q, want preserved key message", message)
		}
		if !strings.Contains(message, "resolve home directory") {
			t.Fatalf("safeConfigErrorMessage() = %q, want reason", message)
		}
	})
}

func equalAttributes(got, want []any) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

type configErrorCause struct{}

func (configErrorCause) Error() string { return "test-only cause" }
