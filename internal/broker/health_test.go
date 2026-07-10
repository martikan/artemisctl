package broker

import "testing"

func TestScalarReply(t *testing.T) {
	t.Run("array-wrapped float", func(t *testing.T) {
		got, err := scalarReply("[0.42]")
		if err != nil || got != 0.42 {
			t.Fatalf("got %v, %v; want 0.42, nil", got, err)
		}
	})
	t.Run("non-string reply", func(t *testing.T) {
		if _, err := scalarReply(123); err == nil {
			t.Fatal("want error for non-string reply")
		}
	})
	t.Run("unparseable string", func(t *testing.T) {
		if _, err := scalarReply("not-json"); err == nil {
			t.Fatal("want error for unparseable reply")
		}
	})
	t.Run("empty array", func(t *testing.T) {
		if _, err := scalarReply("[]"); err == nil {
			t.Fatal("want error for empty array reply")
		}
	})
}

func TestClassify(t *testing.T) {
	cases := []struct {
		disk, mem float64
		blocking  bool
		want      Verdict
	}{
		{10, 10, false, OK},
		{75, 10, false, Degraded},
		{10, 85, false, Degraded},
		{95, 10, false, Critical},
		{10, 10, true, Critical},
		{80, 92, false, Critical},
	}
	for _, c := range cases {
		if got := classify(c.disk, c.mem, c.blocking); got != c.want {
			t.Fatalf("classify(%v,%v,%v)=%s want %s", c.disk, c.mem, c.blocking, got, c.want)
		}
	}
}
