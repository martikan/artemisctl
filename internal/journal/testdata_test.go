package journal

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fixtureDir extracts testdata/artemis-2.42-data.tar.gz into t.TempDir() and
// returns the extracted data/ path. It skips the test with a clear message if
// the tarball is missing (fixtures not yet harvested — run `make fixtures`).
func fixtureDir(t *testing.T) string {
	t.Helper()
	tarball := filepath.Join("testdata", "artemis-2.42-data.tar.gz")
	f, err := os.Open(tarball)
	if err != nil {
		if os.IsNotExist(err) {
			t.Skipf("fixture %s missing; run `make fixtures` to harvest it", tarball)
		}
		t.Fatalf("open fixture: %v", err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gunzip fixture: %v", err)
	}
	defer gz.Close()

	dst := t.TempDir()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read fixture tar: %v", err)
		}
		name := filepath.Clean(hdr.Name)
		if strings.HasPrefix(name, "..") || filepath.IsAbs(name) {
			t.Fatalf("fixture tar has unsafe path %q", hdr.Name)
		}
		path := filepath.Join(dst, name)
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(path, 0o755); err != nil {
				t.Fatalf("extract dir %s: %v", name, err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatalf("extract parent of %s: %v", name, err)
			}
			out, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
			if err != nil {
				t.Fatalf("extract file %s: %v", name, err)
			}
			if _, err := io.Copy(out, tr); err != nil { //nolint:gosec // trusted committed fixture
				out.Close()
				t.Fatalf("extract file %s: %v", name, err)
			}
			if err := out.Close(); err != nil {
				t.Fatalf("close extracted %s: %v", name, err)
			}
		default:
			// Symlinks etc. are not expected in the fixture; fail loudly so a
			// bad harvest is caught rather than silently skipped.
			t.Fatalf("fixture tar has unexpected entry type %d for %q", hdr.Typeflag, hdr.Name)
		}
	}
	return filepath.Join(dst, "data")
}

// TestFixtureDir sanity-checks the committed fixture: it extracts and contains
// the four data subdirectories, and the journal is non-empty.
func TestFixtureDir(t *testing.T) {
	dir := fixtureDir(t)
	for _, sub := range []string{"bindings", "journal", "large-messages", "paging"} {
		fi, err := os.Stat(filepath.Join(dir, sub))
		if err != nil || !fi.IsDir() {
			t.Fatalf("fixture missing %s/: %v", sub, err)
		}
	}
	entries, err := os.ReadDir(filepath.Join(dir, "journal"))
	if err != nil || len(entries) == 0 {
		t.Fatalf("fixture journal/ empty: %v", err)
	}
}
