package store

import (
	"os"
	"strconv"
	"strings"
)

// ckptPath is the sidecar checkpoint file for a store: "<store>.ckpt".
func ckptPath(storePath string) string { return storePath + ".ckpt" }

// LoadCheckpoint returns the last saved redelivery offset for storePath, or 0
// if no checkpoint exists yet (a fresh replay starts from the first record).
func LoadCheckpoint(storePath string) (int64, error) {
	b, err := os.ReadFile(ckptPath(storePath))
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
}

// SaveCheckpoint durably records offset as the redelivery progress for
// storePath. It writes to a temp file and renames it into place so a crash
// mid-write cannot corrupt the checkpoint.
func SaveCheckpoint(storePath string, offset int64) error {
	tmp := ckptPath(storePath) + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatInt(offset, 10)), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, ckptPath(storePath))
}
