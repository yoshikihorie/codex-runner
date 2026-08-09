package domain

import (
	"errors"
	"fmt"
	"testing"
)

func TestErrContractWriteFailed(t *testing.T) {
	if !errors.Is(fmt.Errorf("x: %w", ErrContractWriteFailed), ErrContractWriteFailed) {
		t.Fatal("not identifiable")
	}
}

func TestErrOutputContractViolation(t *testing.T) {
	if !errors.Is(fmt.Errorf("x: %w", ErrOutputContractViolation), ErrOutputContractViolation) {
		t.Fatal("not identifiable")
	}
}
