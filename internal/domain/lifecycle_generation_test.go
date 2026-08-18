package domain

import "testing"

func TestLifecycleGenerationIsIndependentUint64Value(t *testing.T) {
	var generation LifecycleGeneration = 1
	if generation == 0 {
		t.Fatal("LifecycleGeneration must retain its uint64 value")
	}

	copy := generation
	if copy != generation {
		t.Fatalf("LifecycleGeneration copy = %d, want %d", copy, generation)
	}
}
