package main

import (
	"testing"

	"golang.org/x/text/encoding/htmlindex"
)

// TestEncodingGroups checks that the table printed for an unknown --encoding
// lists each canonical htmlindex encoding exactly once.
func TestEncodingGroups(t *testing.T) {
	seen := map[string]bool{}
	for _, g := range encodingGroups {
		for _, name := range g.names {
			enc, err := htmlindex.Get(name)
			if err != nil {
				t.Errorf("%s: %v", name, err)
				continue
			}
			canonical, err := htmlindex.Name(enc)
			if err != nil || canonical != name {
				t.Errorf("%s: canonical name is %q (err %v)", name, canonical, err)
			}
			if seen[name] {
				t.Errorf("%s: listed twice", name)
			}
			seen[name] = true
		}
	}
	if len(seen) != 40 {
		t.Errorf("listed %d encodings, want 40", len(seen))
	}
}
