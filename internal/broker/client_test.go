package broker

import "testing"

func TestConnOptions(t *testing.T) {
	cases := []struct {
		name    string
		in      ConnectionProps
		wantSet bool
	}{
		{"with creds", ConnectionProps{URL: "h:61616", Username: "a", Password: "b"}, true},
		{"special char password", ConnectionProps{URL: "h:61616", Username: "a", Password: "X/Y="}, true},
		{"no creds", ConnectionProps{URL: "h:61616"}, false},
		{"user only", ConnectionProps{URL: "h:61616", Username: "a"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			opts := connOptions(c.in)
			if got := opts != nil && opts.SASLType != nil; got != c.wantSet {
				t.Fatalf("SASL set = %v, want %v", got, c.wantSet)
			}
		})
	}
}
