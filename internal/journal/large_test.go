package journal

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/Azure/go-amqp"
)

// copyDir recursively copies src into dst (both must exist/be creatable);
// used to get a mutable copy of the fixture's large-messages dir for the
// delete/orphan tests, since fixtureDir's extraction must stay read-only per
// this task's "strictly read-only on the source dir" contract.
func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatalf("copyDir: read %s: %v", src, err)
	}
	for _, e := range entries {
		srcPath := filepath.Join(src, e.Name())
		dstPath := filepath.Join(dst, e.Name())
		if e.IsDir() {
			if err := os.MkdirAll(dstPath, 0o755); err != nil {
				t.Fatalf("copyDir: mkdir %s: %v", dstPath, err)
			}
			copyDir(t, srcPath, dstPath)
			continue
		}
		in, err := os.Open(srcPath)
		if err != nil {
			t.Fatalf("copyDir: open %s: %v", srcPath, err)
		}
		out, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			in.Close()
			t.Fatalf("copyDir: create %s: %v", dstPath, err)
		}
		if _, err := io.Copy(out, in); err != nil {
			in.Close()
			out.Close()
			t.Fatalf("copyDir: copy %s: %v", srcPath, err)
		}
		in.Close()
		if err := out.Close(); err != nil {
			t.Fatalf("copyDir: close %s: %v", dstPath, err)
		}
	}
}

// fixtureLargeMessage returns the salvage.large journal-decoded Message
// (Large=true, AMQP empty) from the full fixture pipe, plus the fixture's
// large-messages dir path.
func fixtureLargeMessage(t *testing.T) (Message, string) {
	t.Helper()
	dir := fixtureDir(t)
	messages, _, names := decodeFixtureMessages(t)
	byQ := byQueue(t, messages, names)
	got := byQ["salvage.large"]
	if len(got) != 1 {
		t.Fatalf("want 1 salvage.large message, got %d", len(got))
	}
	return got[0], filepath.Join(dir, "large-messages")
}

func TestAttachLargeBodiesFixtureRoundTrip(t *testing.T) {
	msg, largeDir := fixtureLargeMessage(t)
	man := loadManifest(t)
	want := man.Queues["salvage.large"]
	if len(want) != 1 {
		t.Fatalf("manifest: want 1 salvage.large entry, got %d", len(want))
	}

	out, diag, err := AttachLargeBodies([]Message{msg}, largeDir)
	if err != nil {
		t.Fatalf("AttachLargeBodies: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("want 1 message, got %d", len(out))
	}
	if len(diag.MissingFile) != 0 {
		t.Errorf("MissingFile = %v, want empty", diag.MissingFile)
	}
	if len(diag.Orphans) != 0 {
		t.Errorf("Orphans = %v, want empty", diag.Orphans)
	}

	// format_notes.md section 5b: the .msg file is the complete AMQP-encoded
	// message (Header...Data...), not just the body -- so msg.AMQP must be
	// the file bytes verbatim, round-trippable via go-amqp's UnmarshalBinary,
	// and its Data section must match the manifest's sha256/len.
	var am amqp.Message
	if err := am.UnmarshalBinary(out[0].AMQP); err != nil {
		t.Fatalf("unmarshal joined AMQP: %v", err)
	}
	body := am.GetData()
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	if hash != want[0].BodySha256 {
		t.Errorf("body sha256 = %s, want %s", hash, want[0].BodySha256)
	}
	if len(body) != want[0].BodyLen {
		t.Errorf("body len = %d, want %d", len(body), want[0].BodyLen)
	}
	if len(body) != 307200 {
		t.Errorf("body len = %d, want 307200 (manifest-documented)", len(body))
	}

	if diag.LargestBytes < 307200 {
		t.Errorf("LargestBytes = %d, want >= 307200", diag.LargestBytes)
	}
}

func TestAttachLargeBodiesMissingFileDropsMessage(t *testing.T) {
	msg, largeDir := fixtureLargeMessage(t)

	tmpDir := t.TempDir()
	copyDir(t, largeDir, tmpDir)
	if err := os.Remove(filepath.Join(tmpDir, "64.msg")); err != nil {
		t.Fatalf("remove fixture copy's 64.msg: %v", err)
	}

	out, diag, err := AttachLargeBodies([]Message{msg}, tmpDir)
	if err != nil {
		t.Fatalf("AttachLargeBodies: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("want message dropped, got %d: %+v", len(out), out)
	}
	if len(diag.MissingFile) != 1 || diag.MissingFile[0] != msg.ID {
		t.Errorf("MissingFile = %v, want [%d]", diag.MissingFile, msg.ID)
	}
	if len(diag.Orphans) != 0 {
		t.Errorf("Orphans = %v, want empty", diag.Orphans)
	}
}

func TestAttachLargeBodiesOrphanFileReported(t *testing.T) {
	msg, largeDir := fixtureLargeMessage(t)

	tmpDir := t.TempDir()
	copyDir(t, largeDir, tmpDir)
	orphanPath := filepath.Join(tmpDir, "999999.msg")
	if err := os.WriteFile(orphanPath, []byte("orphan body"), 0o644); err != nil {
		t.Fatalf("write orphan file: %v", err)
	}

	out, diag, err := AttachLargeBodies([]Message{msg}, tmpDir)
	if err != nil {
		t.Fatalf("AttachLargeBodies: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("want 1 message (unaffected by orphan), got %d", len(out))
	}
	if len(diag.MissingFile) != 0 {
		t.Errorf("MissingFile = %v, want empty", diag.MissingFile)
	}
	if len(diag.Orphans) != 1 || diag.Orphans[0] != "999999.msg" {
		t.Errorf("Orphans = %v, want [999999.msg]", diag.Orphans)
	}

	// The surviving message's own body must be unaffected by the orphan.
	var am amqp.Message
	if err := am.UnmarshalBinary(out[0].AMQP); err != nil {
		t.Fatalf("unmarshal joined AMQP: %v", err)
	}
	if len(am.GetData()) != 307200 {
		t.Errorf("body len = %d, want 307200", len(am.GetData()))
	}
}

func TestAttachLargeBodiesIgnoresNonMsgJunk(t *testing.T) {
	msg, largeDir := fixtureLargeMessage(t)

	tmpDir := t.TempDir()
	copyDir(t, largeDir, tmpDir)
	// Non-.msg junk (e.g. a stray .tmp from an interrupted write, or a
	// directory) must not be reported as an orphan or otherwise disturb the
	// result -- only *.msg files are large-message bodies.
	if err := os.WriteFile(filepath.Join(tmpDir, "notes.txt"), []byte("junk"), 0o644); err != nil {
		t.Fatalf("write junk file: %v", err)
	}
	if err := os.Mkdir(filepath.Join(tmpDir, "subdir"), 0o755); err != nil {
		t.Fatalf("mkdir junk subdir: %v", err)
	}

	out, diag, err := AttachLargeBodies([]Message{msg}, tmpDir)
	if err != nil {
		t.Fatalf("AttachLargeBodies: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("want 1 message, got %d", len(out))
	}
	if len(diag.MissingFile) != 0 {
		t.Errorf("MissingFile = %v, want empty", diag.MissingFile)
	}
	if len(diag.Orphans) != 0 {
		t.Errorf("Orphans = %v, want empty (non-.msg files are silently ignored)", diag.Orphans)
	}
}

func TestAttachLargeBodiesEmptyInput(t *testing.T) {
	out, diag, err := AttachLargeBodies(nil, t.TempDir())
	if err != nil {
		t.Fatalf("AttachLargeBodies: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("out = %v, want empty", out)
	}
	if len(diag.MissingFile) != 0 || len(diag.Orphans) != 0 || diag.LargestBytes != 0 {
		t.Errorf("diag = %+v, want zero", diag)
	}
}

// TestAttachLargeBodiesLeavesNonLargeMessagesUntouched: a non-Large message
// passed through must survive unchanged (defensive -- callers are expected
// to filter to Large-only, but the function should not corrupt other
// messages if handed a mixed slice).
func TestAttachLargeBodiesLeavesNonLargeMessagesUntouched(t *testing.T) {
	plain := Message{ID: 999, AMQP: []byte("already-here"), QueueIDs: []int64{1}}

	out, diag, err := AttachLargeBodies([]Message{plain}, t.TempDir())
	if err != nil {
		t.Fatalf("AttachLargeBodies: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("want 1 message, got %d", len(out))
	}
	if string(out[0].AMQP) != "already-here" {
		t.Errorf("AMQP = %q, want unchanged", out[0].AMQP)
	}
	if len(diag.MissingFile) != 0 || len(diag.Orphans) != 0 {
		t.Errorf("diag = %+v, want zero", diag)
	}
}

// TestAttachLargeBodiesNonexistentDirWithLargeMessage: a missing large-messages/
// directory is acceptable (not fatal). Large messages are dropped (file not found)
// and recorded in MissingFile.
func TestAttachLargeBodiesNonexistentDirWithLargeMessage(t *testing.T) {
	largeMsg := Message{ID: 64, Large: true, QueueIDs: []int64{1}}
	nonexistentDir := filepath.Join(t.TempDir(), "nonexistent-large-messages")

	out, diag, err := AttachLargeBodies([]Message{largeMsg}, nonexistentDir)
	if err != nil {
		t.Fatalf("AttachLargeBodies: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("want message dropped, got %d: %+v", len(out), out)
	}
	if len(diag.MissingFile) != 1 || diag.MissingFile[0] != 64 {
		t.Errorf("MissingFile = %v, want [64]", diag.MissingFile)
	}
	if len(diag.Orphans) != 0 {
		t.Errorf("Orphans = %v, want empty", diag.Orphans)
	}
}

// TestAttachLargeBodiesNonexistentDirWithNonLargeMessages: a missing large-messages/
// directory is acceptable. Non-Large messages pass through unchanged with empty diag.
func TestAttachLargeBodiesNonexistentDirWithNonLargeMessages(t *testing.T) {
	msg1 := Message{ID: 100, AMQP: []byte("msg1-body"), QueueIDs: []int64{1}}
	msg2 := Message{ID: 200, AMQP: []byte("msg2-body"), QueueIDs: []int64{1}}
	nonexistentDir := filepath.Join(t.TempDir(), "nonexistent-large-messages")

	out, diag, err := AttachLargeBodies([]Message{msg1, msg2}, nonexistentDir)
	if err != nil {
		t.Fatalf("AttachLargeBodies: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("want 2 messages, got %d", len(out))
	}
	if string(out[0].AMQP) != "msg1-body" {
		t.Errorf("out[0].AMQP = %q, want unchanged", out[0].AMQP)
	}
	if string(out[1].AMQP) != "msg2-body" {
		t.Errorf("out[1].AMQP = %q, want unchanged", out[1].AMQP)
	}
	if len(diag.MissingFile) != 0 || len(diag.Orphans) != 0 || diag.LargestBytes != 0 {
		t.Errorf("diag = %+v, want zero", diag)
	}
}
