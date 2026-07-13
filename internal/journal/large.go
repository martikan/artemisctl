package journal

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// LargeDiag reports large-message joining problems.
type LargeDiag struct {
	MissingFile  []int64  // messageIDs whose <id>.msg is absent (skip + report)
	Orphans      []string // .msg files with no surviving journal record (normal; report only)
	LargestBytes int64    // size of the largest successfully attached large-message body
}

// AttachLargeBodies joins each msg.Large message with its
// <dir>/<ID>.msg body, producing the complete raw AMQP bytes in msg.AMQP.
//
// format_notes.md section 5b (fixture-verified): "data/large-messages/<id>.msg
// holds the complete AMQP-encoded message (Header + Properties + ... + Data
// section with the full body) -- fixture 64.msg is 307255 bytes = 55 bytes of
// sections + 307200 body, and it starts with 00 53 70 (Header descriptor),
// not raw body bytes. So msg.AMQP = the .msg file bytes verbatim; the journal
// record's saved-encoding block is only a section index / header
// cross-check." This is the opposite of the brief's fallback branch (journal
// header + file body concatenation) -- that branch does not apply here.
//
// Messages whose file is missing are removed from the returned slice and
// their ID recorded in LargeDiag.MissingFile. Non-Large messages pass through
// unchanged. Bodies are assembled in RAM (spec: accepted v1 cost).
//
// Strictly read-only on dir: only os.ReadDir/os.ReadFile are used.
func AttachLargeBodies(msgs []Message, dir string) ([]Message, LargeDiag, error) {
	var diag LargeDiag

	entries, err := os.ReadDir(dir)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, diag, fmt.Errorf("journal: read large-messages dir %s: %w", dir, err)
		}
		// Missing large-messages/ directory is acceptable (no large bodies to attach).
		// Large messages will be dropped when their individual .msg files are not found.
		entries = nil
	}

	// referenced tracks which <id>.msg files are actually claimed by a
	// surviving Large message, so any leftover *.msg is reported as an
	// orphan (format_notes.md: normal, not an error -- the large message's
	// journal record may itself have been acked/deleted while the body file
	// lingers on disk).
	referenced := make(map[string]bool)
	for _, m := range msgs {
		if isLarge, _ := m.largeBodyTarget(); isLarge {
			referenced[strconv.FormatInt(m.ID, 10)+".msg"] = true
		}
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		// Non-.msg junk (partial writes, stray files) is silently ignored --
		// only *.msg files are large-message bodies (brief Step: "decide
		// handling (ignore silently vs orphan-report) and document").
		if !strings.HasSuffix(name, ".msg") {
			continue
		}
		if !referenced[name] {
			diag.Orphans = append(diag.Orphans, name)
		}
	}

	out := make([]Message, 0, len(msgs))
	for _, m := range msgs {
		isLarge, isCore := m.largeBodyTarget()
		if !isLarge {
			out = append(out, m)
			continue
		}

		path := filepath.Join(dir, strconv.FormatInt(m.ID, 10)+".msg")
		body, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				diag.MissingFile = append(diag.MissingFile, m.ID)
				continue
			}
			return nil, diag, fmt.Errorf("journal: read large-message body %s: %w", path, err)
		}

		// AMQP large bodies are the complete AMQP-encoded message (msg.AMQP);
		// Core large bodies are the raw message body bytes (msg.Core.Body).
		if isCore {
			m.Core.Body = body
		} else {
			m.AMQP = body
		}
		if int64(len(body)) > diag.LargestBytes {
			diag.LargestBytes = int64(len(body))
		}
		out = append(out, m)
	}

	return out, diag, nil
}
