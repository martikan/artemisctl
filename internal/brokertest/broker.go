// Package brokertest provides a single shared Artemis broker container reused
// by every integration test across packages. It lives in a normal (non-_test)
// package because test-only symbols cannot be shared across package boundaries.
package brokertest

import (
	"context"
	"testing"
	"time"

	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Conn holds the address and credentials of the shared broker. It is a plain
// struct rather than broker.ConnectionProps so this package does not import
// internal/broker: an internal (package broker) test importing a package that
// imports broker would be an import cycle. Callers map it to ConnectionProps.
type Conn struct {
	URL      string
	Username string
	Password string
}

const (
	// sharedName is the fixed container name that makes Reuse attach to one
	// instance across test binaries instead of starting a new one each time.
	sharedName = "artemisctl-it-broker"

	// sharedImage is recent enough to expose the two-arg JSON
	// addAddressSettings(String,String) overload cordon relies on (2.33+) and
	// boots reliably under the NIO journal.
	sharedImage = "apache/activemq-artemis:2.42.0-alpine"

	sharedUser = "artemis"
	sharedPass = "artemis"
)

// PermissiveWildcardSettings is an address-settings JSON for the wildcard match
// that disables the address-full limits, used to lift any leftover cordon at the
// start of each test. Applying this via Client.Uncordon reliably overrides a
// cordon's FAIL policy — unlike removeAddressSettings("#"), which reports success
// but does not actually clear the wildcard override on Artemis 2.42. The DLA,
// expiry, and auto-create fields match the broker defaults so tests that rely on
// DLQ/ExpiryQueue and queue auto-creation keep working.
const PermissiveWildcardSettings = `{"addressFullMessagePolicy":"PAGE","maxSizeBytes":104857600,"maxReadPageBytes":20971520,"maxReadPageMessages":-1,"pageLimitBytes":-1,"pageLimitMessages":-1,"maxSizeMessages":-1,"pageSizeBytes":10485760,"messageCounterHistoryDayLimit":10,"redeliveryDelay":0,"deadLetterAddress":"DLQ","expiryAddress":"ExpiryQueue","slowConsumerThresholdMeasurementUnit":"MESSAGES_PER_SECOND","autoCreateQueues":true,"autoDeleteQueues":false,"autoCreateAddresses":true,"autoDeleteAddresses":false,"managementBrowsePageSize":200,"maxSizeBytesRejectThreshold":-1}`

// Shared starts, or attaches to, the single reused Artemis broker used by all
// integration tests and returns connection props for it.
//
// Dirty context: the container is deliberately NOT terminated. Reused
// containers are excluded from the Ryuk reaper, so the broker persists across
// tests and test binaries; the first caller starts it and every later caller
// (in either package) attaches by name. Because state accumulates, tests must
// use distinct queue/address names, and cordon tests must restore their
// settings so the broker is left usable for the next test.
func Shared(t testing.TB) Conn {
	t.Helper()
	ctx := context.Background()
	req := tc.ContainerRequest{
		Name:         sharedName,
		Image:        sharedImage,
		ExposedPorts: []string{"61616/tcp"},
		Env: map[string]string{
			"ARTEMIS_USER":     sharedUser,
			"ARTEMIS_PASSWORD": sharedPass,
			// --nio: newer images default to the AIO journal, which fails
			// io_getevents under rootless container runtimes; NIO boots
			// everywhere. --relax-jolokia keeps the management console reachable.
			"EXTRA_ARGS": "--nio --relax-jolokia",
		},
		WaitingFor: wait.ForListeningPort("61616/tcp").WithStartupTimeout(120 * time.Second),
	}
	ctr, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            true,
	})
	if err != nil {
		t.Fatalf("start shared artemis: %v", err)
	}
	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "61616/tcp")
	if err != nil {
		t.Fatalf("port: %v", err)
	}
	return Conn{URL: host + ":" + port.Port(), Username: sharedUser, Password: sharedPass}
}
