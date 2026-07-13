package broker

import "testing"

func TestBuildConnectionURL(t *testing.T) {
	cases := []struct {
		name string
		in   ConnectionProps
		want string
	}{
		{"with creds", ConnectionProps{URL: "h:61616", Username: "a", Password: "b"}, "a:b@h:61616"},
		{"no creds", ConnectionProps{URL: "h:61616"}, "h:61616"},
		{"user only", ConnectionProps{URL: "h:61616", Username: "a"}, "h:61616"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := buildConnectionURL(c.in); got != c.want {
				t.Fatalf("got %q want %q", got, c.want)
			}
		})
	}
}
