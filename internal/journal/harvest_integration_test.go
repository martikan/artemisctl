package journal_test

// Harvests a real Artemis 2.42 data dir into testdata/. Run via `make fixtures`.
//
// This is a fixture *generator* that happens to run under `go test` so it can
// reuse the repo's testcontainers plumbing. It is gated behind
// ARTEMISCTL_HARVEST=1 so `make test` never runs it. It uses a DEDICATED
// container (not brokertest.Shared): this flow stops the broker, which would
// break the shared container's Reuse contract.
//
// Output:
//   - testdata/artemis-2.42-data.tar.gz : the broker's complete data/ dir
//     (bindings/ journal/ large-messages/ paging/), tar paths rooted at "data/"
//   - testdata/manifest.json            : every produced message (sha256/len/props)

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/docker/docker/api/types/container"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/martikan/artemisctl/internal/broker"
	"github.com/martikan/artemisctl/internal/brokertest"
)

// instanceDataDir is where the apache/activemq-artemis image keeps the broker
// instance's data directory; verified by exec'ing `ls` in the running
// container (the harvest fails loudly if the layout differs).
const instanceDataDir = "/var/lib/artemis-instance/data"

// manifestEntry describes one produced message, schema per the task brief.
type manifestEntry struct {
	BodySha256    string         `json:"bodySha256"`
	BodyLen       int            `json:"bodyLen"`
	Props         map[string]any `json:"props,omitempty"`
	ScheduledAtMs int64          `json:"scheduledAtMs,omitempty"`
}

type manifest struct {
	Queues map[string][]manifestEntry `json:"queues"`
}

// scheduledAtMs is 2100-01-01T00:00:00Z in unix millis — far enough in the
// future that the scheduled message never becomes deliverable during harvest.
const scheduledAtMs = int64(4102444800000)

// fixtureBody builds the deterministic body for message i of a queue:
// fmt.Sprintf("salvage-fixture-%s-%04d|", queue, i) repeated (and truncated)
// to exactly n bytes.
func fixtureBody(queue string, i, n int) []byte {
	unit := fmt.Sprintf("salvage-fixture-%s-%04d|", queue, i)
	b := make([]byte, 0, n+len(unit))
	for len(b) < n {
		b = append(b, unit...)
	}
	return b[:n]
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func TestHarvestFixture(t *testing.T) {
	if os.Getenv("ARTEMISCTL_HARVEST") != "1" {
		t.Skip("fixture harvester; run via make fixtures")
	}
	ctx := context.Background()

	// 1. Dedicated broker with a named volume over its data dir, so the data
	// survives container removal and can be copied out by a helper container.
	vol := fmt.Sprintf("artemisctl-harvest-%d", time.Now().Unix())
	req := tc.ContainerRequest{
		Image:        "apache/activemq-artemis:2.42.0-alpine",
		ExposedPorts: []string{"61616/tcp"},
		Env: map[string]string{
			"ARTEMIS_USER":     "artemis",
			"ARTEMIS_PASSWORD": "artemis",
			// --nio: AIO fails under rootless container runtimes (see
			// brokertest). --relax-jolokia keeps management reachable.
			"EXTRA_ARGS": "--nio --relax-jolokia",
		},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.Binds = append(hc.Binds, vol+":"+instanceDataDir)
		},
		WaitingFor: wait.ForListeningPort("61616/tcp").WithStartupTimeout(120 * time.Second),
	}
	ctr, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatalf("start artemis: %v", err)
	}
	defer func() {
		_ = ctr.Terminate(context.Background())
		// The named volume is not managed by testcontainers; remove it by hand.
		_ = exec.Command("docker", "volume", "rm", "-f", vol).Run()
	}()

	// Verify the image's data-dir layout before trusting it: the journal must
	// be created under the mounted path or the harvest would capture nothing.
	requireDataDirLayout(t, ctx, ctr)

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "61616/tcp")
	if err != nil {
		t.Fatalf("port: %v", err)
	}
	props := broker.ConnectionProps{URL: host + ":" + port.Port(), Username: "artemis", Password: "artemis"}

	connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	c, err := broker.Connect(connectCtx, props)
	cancel()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	// 2. Force paging on salvage.paged BEFORE producing to it: 500 x 1 KiB
	// >> 64 KiB maxSize with the PAGE policy => multiple 16 KiB page files.
	pagedSettings, err := withPagingPolicy(brokertest.PermissiveWildcardSettings, 65536, 16384)
	if err != nil {
		t.Fatalf("build paged settings: %v", err)
	}
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = c.ApplyAddressSettings(opCtx, "salvage.paged", pagedSettings)
	cancel()
	if err != nil {
		t.Fatalf("apply paging settings: %v", err)
	}

	// 3. Produce the known message set. Every body is deterministic; the
	// manifest records sha256 + len (+ props / scheduled time).
	man := manifest{Queues: map[string][]manifestEntry{}}
	sendCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	// salvage.plain: 5 durable text messages, 64 B.
	man.Queues["salvage.plain"] = sendBatch(t, sendCtx, c, "salvage.plain", 5, 64, nil)

	// salvage.props: 5 durable messages with application properties and priority 7.
	man.Queues["salvage.props"] = sendBatch(t, sendCtx, c, "salvage.props", 5, 64, func(m *amqp.Message, e *manifestEntry) {
		m.ApplicationProperties = map[string]any{"region": "eu", "attempt": int32(1)}
		m.Header.Priority = 7
		e.Props = map[string]any{"region": "eu", "attempt": 1}
	})

	// salvage.scheduled: 1 durable message scheduled for 2100-01-01.
	man.Queues["salvage.scheduled"] = sendBatch(t, sendCtx, c, "salvage.scheduled", 1, 64, func(m *amqp.Message, e *manifestEntry) {
		m.Annotations = amqp.Annotations{"x-opt-delivery-time": scheduledAtMs}
		e.ScheduledAtMs = scheduledAtMs
	})

	// salvage.large: 1 durable message, 300 KiB — over the 100 KiB
	// amqpMinLargeMessageSize default, so stored as a large message.
	man.Queues["salvage.large"] = sendBatch(t, sendCtx, c, "salvage.large", 1, 300*1024, nil)

	// salvage.acked: 3 messages, then receive and accept all 3 — leaves
	// ADD_REF + ACKNOWLEDGE_REF material so replay can eliminate them.
	sendBatch(t, sendCtx, c, "salvage.acked", 3, 64, nil)
	receiveAndAccept(t, sendCtx, c, "salvage.acked", 3)
	man.Queues["salvage.acked"] = []manifestEntry{}

	// salvage.paged: 500 durable 1 KiB messages into the paging-forced address.
	man.Queues["salvage.paged"] = sendBatch(t, sendCtx, c, "salvage.paged", 500, 1024, nil)

	closeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	err = c.Close(closeCtx)
	cancel()
	if err != nil {
		t.Fatalf("close client: %v", err)
	}

	// 4. Verify paging actually happened before freezing the fixture.
	requireNonEmptyPaging(t, ctx, ctr)

	// 5. Clean shutdown with a generous timeout so Artemis closes the journal
	// cleanly (SIGTERM => broker Stop).
	stopTimeout := 60 * time.Second
	if err := ctr.Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("stop broker: %v", err)
	}

	// 6. Copy the volume out through a busybox helper (the volume itself is
	// not directly readable from the host under rootless runtimes).
	out := copyVolumeOut(t, ctx, vol)

	// 7. Tar+gzip => testdata/artemis-2.42-data.tar.gz, rooted at "data/".
	testdata := filepath.Join(mustModuleDir(t), "internal", "journal", "testdata")
	if err := os.MkdirAll(testdata, 0o755); err != nil {
		t.Fatalf("mkdir testdata: %v", err)
	}
	tarball := filepath.Join(testdata, "artemis-2.42-data.tar.gz")
	if err := tarGzDir(out, "data", tarball); err != nil {
		t.Fatalf("pack tarball: %v", err)
	}
	fi, err := os.Stat(tarball)
	if err != nil {
		t.Fatalf("stat tarball: %v", err)
	}
	t.Logf("tarball: %s (%d bytes)", tarball, fi.Size())
	if fi.Size() > 5*1024*1024 {
		t.Fatalf("fixture tarball is %d bytes (> 5 MB): too big to commit, investigate", fi.Size())
	}

	// 8. Write the manifest.
	mb, err := json.MarshalIndent(man, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(testdata, "manifest.json"), append(mb, '\n'), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
}

// sendBatch sends n durable messages of bodyLen bytes to queue and returns
// their manifest entries. customize (optional) mutates each message and its
// manifest entry before sending.
func sendBatch(t *testing.T, ctx context.Context, c *broker.Client, queue string, n, bodyLen int, customize func(*amqp.Message, *manifestEntry)) []manifestEntry {
	t.Helper()
	// TargetCapabilities "queue" is REQUIRED: without it Artemis routes the
	// send as multicast and the message never lands in the anycast queue.
	sender, err := c.Session().NewSender(ctx, queue, &amqp.SenderOptions{TargetCapabilities: []string{"queue"}})
	if err != nil {
		t.Fatalf("open sender %s: %v", queue, err)
	}
	defer sender.Close(context.Background())

	entries := make([]manifestEntry, 0, n)
	for i := 0; i < n; i++ {
		body := fixtureBody(queue, i, bodyLen)
		msg := amqp.NewMessage(body)
		msg.Header = &amqp.MessageHeader{Durable: true}
		msg.Properties = &amqp.MessageProperties{MessageID: fmt.Sprintf("%s-%04d", queue, i)}
		e := manifestEntry{BodySha256: sha256Hex(body), BodyLen: len(body)}
		if customize != nil {
			customize(msg, &e)
		}
		if err := sender.Send(ctx, msg, nil); err != nil {
			t.Fatalf("send %s[%d]: %v", queue, i, err)
		}
		entries = append(entries, e)
	}
	return entries
}

// receiveAndAccept receives and accepts n messages from queue.
func receiveAndAccept(t *testing.T, ctx context.Context, c *broker.Client, queue string, n int) {
	t.Helper()
	recv, err := c.Session().NewReceiver(ctx, queue, &amqp.ReceiverOptions{
		SourceCapabilities: []string{"queue"},
	})
	if err != nil {
		t.Fatalf("open receiver %s: %v", queue, err)
	}
	defer recv.Close(context.Background())
	for i := 0; i < n; i++ {
		msg, err := recv.Receive(ctx, nil)
		if err != nil {
			t.Fatalf("receive %s[%d]: %v", queue, i, err)
		}
		if err := recv.AcceptMessage(ctx, msg); err != nil {
			t.Fatalf("accept %s[%d]: %v", queue, i, err)
		}
	}
}

// withPagingPolicy returns base (a settings JSON object) with the address-full
// policy forced to PAGE and the given size thresholds, leaving all other
// fields (DLA, expiry, auto-create, ...) intact.
func withPagingPolicy(base string, maxSizeBytes, pageSizeBytes int) (string, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(base), &m); err != nil {
		return "", fmt.Errorf("parse base settings: %w", err)
	}
	m["addressFullMessagePolicy"] = json.RawMessage(`"PAGE"`)
	m["maxSizeBytes"] = json.RawMessage(fmt.Sprintf("%d", maxSizeBytes))
	m["pageSizeBytes"] = json.RawMessage(fmt.Sprintf("%d", pageSizeBytes))
	out, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("marshal paged settings: %w", err)
	}
	return string(out), nil
}

// execOut runs cmd in the container and returns its combined output.
func execOut(t *testing.T, ctx context.Context, ctr tc.Container, cmd []string) (int, string) {
	t.Helper()
	code, rd, err := ctr.Exec(ctx, cmd)
	if err != nil {
		t.Fatalf("exec %v: %v", cmd, err)
	}
	b, err := io.ReadAll(rd)
	if err != nil {
		t.Fatalf("exec %v read: %v", cmd, err)
	}
	return code, string(b)
}

// requireDataDirLayout fails the harvest unless the running broker keeps its
// journal under instanceDataDir (i.e. under our named volume).
func requireDataDirLayout(t *testing.T, ctx context.Context, ctr tc.Container) {
	t.Helper()
	code, out := execOut(t, ctx, ctr, []string{"ls", instanceDataDir})
	if code != 0 {
		_, alt := execOut(t, ctx, ctr, []string{"ls", "/var/lib/artemis-instance"})
		t.Fatalf("data dir %s not found (exit %d); instance layout:\n%s", instanceDataDir, code, alt)
	}
	for _, want := range []string{"journal", "bindings", "large-messages", "paging"} {
		if !strings.Contains(out, want) {
			t.Fatalf("data dir %s missing %q; contents:\n%s", instanceDataDir, want, out)
		}
	}
}

// requireNonEmptyPaging fails the harvest unless the paging dir contains a
// non-empty address dir with at least 2 page files.
func requireNonEmptyPaging(t *testing.T, ctx context.Context, ctr tc.Container) {
	t.Helper()
	code, out := execOut(t, ctx, ctr, []string{"sh", "-c", "ls " + instanceDataDir + "/paging/*/ 2>/dev/null"})
	if code != 0 || strings.TrimSpace(out) == "" {
		_, top := execOut(t, ctx, ctr, []string{"ls", "-la", instanceDataDir + "/paging"})
		t.Fatalf("paging did not happen: no address dir under %s/paging (exit %d)\npaging dir:\n%s", instanceDataDir, code, top)
	}
	pages := 0
	for _, line := range strings.Fields(out) {
		if strings.HasSuffix(line, ".page") {
			pages++
		}
	}
	if pages < 2 {
		t.Fatalf("paging produced %d page files (< 2); paging listing:\n%s", pages, out)
	}
	t.Logf("paging OK: %d page files", pages)
}

// copyVolumeOut runs a busybox helper container that mounts the named volume
// read-only plus a host bind dir and copies the volume's contents out. It
// returns the host dir holding the copied data/.
func copyVolumeOut(t *testing.T, ctx context.Context, vol string) string {
	t.Helper()
	out := t.TempDir()
	helperReq := tc.ContainerRequest{
		Image: "busybox:1.36",
		// cp -r (not -a): under rootless podman the helper's root maps to the
		// host user, but -a would preserve the artemis UID (an unmapped
		// subuid), leaving files the host user cannot delete when t.TempDir
		// cleans up. a+rwX keeps everything readable and removable.
		Cmd: []string{"sh", "-c", "cp -r /vol/. /out/ && chmod -R a+rwX /out"},
		HostConfigModifier: func(hc *container.HostConfig) {
			// :Z relabels the bind for SELinux hosts (Fedora); harmless elsewhere.
			hc.Binds = append(hc.Binds, vol+":/vol:ro", out+":/out:Z")
		},
		WaitingFor: wait.ForExit().WithExitTimeout(60 * time.Second),
	}
	helper, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{ContainerRequest: helperReq, Started: true})
	if err != nil {
		t.Fatalf("run copy helper: %v", err)
	}
	defer helper.Terminate(context.Background())
	state, err := helper.State(ctx)
	if err != nil {
		t.Fatalf("helper state: %v", err)
	}
	if state.ExitCode != 0 {
		rd, lerr := helper.Logs(ctx)
		logs := ""
		if lerr == nil {
			b, _ := io.ReadAll(rd)
			logs = string(b)
		}
		t.Fatalf("copy helper exited %d; logs:\n%s", state.ExitCode, logs)
	}
	// Sanity: the copy must contain the journal dir.
	if _, err := os.Stat(filepath.Join(out, "journal")); err != nil {
		entries, _ := os.ReadDir(out)
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("copied volume has no journal/ (contents: %v): %v", names, err)
	}
	return out
}

// tarGzDir packs srcDir into a gzipped tar at dest, with all paths rooted at
// root/ (e.g. "data/journal/activemq-data-1.amq").
func tarGzDir(srcDir, root, dest string) error {
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewWriterLevel(f, gzip.BestCompression)
	if err != nil {
		return err
	}
	tw := tar.NewWriter(gz)

	err = filepath.Walk(srcDir, func(path string, info os.FileInfo, werr error) error {
		if werr != nil {
			return werr
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		name := root
		if rel != "." {
			name = root + "/" + filepath.ToSlash(rel)
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = name
		if info.IsDir() {
			hdr.Name += "/"
		}
		// Normalize ownership for a reproducible, committable fixture.
		hdr.Uid, hdr.Gid = 0, 0
		hdr.Uname, hdr.Gname = "", ""
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if info.IsDir() || !info.Mode().IsRegular() {
			return nil
		}
		src, err := os.Open(path)
		if err != nil {
			return err
		}
		defer src.Close()
		_, err = io.Copy(tw, src)
		return err
	})
	if err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}
	return f.Close()
}

// mustModuleDir returns the repo root (the dir containing go.mod), so the
// harvester writes testdata/ at a stable path regardless of test cwd.
func mustModuleDir(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above test dir")
		}
		dir = parent
	}
}
