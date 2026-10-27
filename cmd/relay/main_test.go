package main

import (
	"testing"
)

func TestParseQueues(t *testing.T) {
	qs, err := parseQueues("default, mail=4,images=2", 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(qs) != 3 || qs[0].Name != "default" || qs[0].Concurrency != 8 ||
		qs[1].Name != "mail" || qs[1].Concurrency != 4 || qs[2].Name != "images" || qs[2].Concurrency != 2 {
		t.Fatalf("parsed = %+v", qs)
	}
	for _, bad := range []string{"", "mail=0", "mail=x", ",,"} {
		if _, err := parseQueues(bad, 8); err == nil {
			t.Errorf("parseQueues(%q) should fail", bad)
		}
	}
}

func TestNewLogger(t *testing.T) {
	if _, err := newLogger("json", "debug"); err != nil {
		t.Fatal(err)
	}
	if _, err := newLogger("yaml", "info"); err == nil {
		t.Fatal("expected error for bad format")
	}
	if _, err := newLogger("text", "loud"); err == nil {
		t.Fatal("expected error for bad level")
	}
}
