package model

import "testing"

func TestTerminal(t *testing.T) {
	for _, s := range []ProcessState{ProcessExited, ProcessFailed} {
		if !(Process{State: s}).Terminal() {
			t.Fatal(s)
		}
	}
	if (Process{State: ProcessUnknown}).Terminal() {
		t.Fatal("unknown must not be terminal")
	}
}
