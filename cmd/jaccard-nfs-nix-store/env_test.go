package main

import (
	"os"
	"testing"
)

// unsetenv removes a variable for the test. t.Setenv was called for it
// before, which is what puts back what was there.
func unsetenv(t *testing.T, name string) {
	t.Helper()
	if err := os.Unsetenv(name); err != nil {
		t.Fatal(err)
	}
}
