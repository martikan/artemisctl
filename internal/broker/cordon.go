package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// wildcardMatch is the address-settings match that covers every address on the
// broker, so a single settings entry cordons the whole broker at once.
const wildcardMatch = "#"

// Cordon thresholds applied to the wildcard match. We use the FAIL address-full
// policy, not BLOCK: BLOCK relies on producer credit-throttling that did not
// reliably engage for AMQP producers in testing (messages kept flowing well past
// the limit), whereas FAIL deterministically rejects a send once the address is
// full, surfacing to the producer as amqp:resource-limit-exceeded ("Address ...
// is full"). maxSizeBytes must be > 0 (Artemis guards its fullness check with
// maxSize > 0) and pageSizeBytes must be < maxSizeBytes (validated).
// maxSizeBytesRejectThreshold is what the FAIL policy actually rejects on.
//
// Caveat: the first message to an otherwise-empty address is accepted (its size
// starts at 0), then every subsequent send is rejected — so at most one small
// message per address can slip in right as the cordon takes hold.
const (
	cordonMaxSizeBytes    = 100
	cordonPageSizeBytes   = 50
	cordonRejectThreshold = 100
)

// uncordonPageSizeBytes is Artemis's default page size, restored by
// UncordonRemove when it resets the wildcard match to a permissive policy.
const uncordonPageSizeBytes = 10485760

// ErrBrokerTooOld is returned by Cordon when the broker does not expose the
// two-argument addAddressSettings(String,String) JSON overload over the AMQP
// management address. On such brokers (e.g. 2.31.x) the setting can only be
// applied via JMX/Jolokia, not AMQP.
var ErrBrokerTooOld = fmt.Errorf("broker does not support addAddressSettings over AMQP management; " +
	"it needs a version with the JSON addAddressSettings(String,String) overload (2.33+)")

// getWildcardSettings reads the current address-settings object for the
// wildcard match and returns it as a JSON object string. Artemis returns this
// double-encoded — an outer JSON array whose single element is itself the JSON
// string of the settings object (["{...}"]) — so we unwrap one level, the same
// shape parseQueueStatsReply handles.
func (c *Client) getWildcardSettings(ctx context.Context) (string, error) {
	body := fmt.Sprintf(`[%q]`, wildcardMatch)
	reply, err := c.callManagement(ctx, "broker", "getAddressSettingsAsJSON", body)
	if err != nil {
		return "", err
	}
	raw, ok := reply.Value.(string)
	if !ok {
		return "", fmt.Errorf("unexpected getAddressSettingsAsJSON reply type %T", reply.Value)
	}
	var outer []string
	if err := json.Unmarshal([]byte(raw), &outer); err != nil {
		return "", fmt.Errorf("parse getAddressSettingsAsJSON reply: %w", err)
	}
	if len(outer) == 0 {
		return "", fmt.Errorf("getAddressSettingsAsJSON returned an empty array")
	}
	return outer[0], nil
}

// applyAddressSettings applies a settings object (JSON) to a match via the
// two-arg addAddressSettings(String,String) overload. It translates the
// "no operation addAddressSettings/2" rejection from an older broker into the
// clearer ErrBrokerTooOld.
func (c *Client) applyAddressSettings(ctx context.Context, match, settingsJSON string) error {
	body, err := json.Marshal([]interface{}{match, settingsJSON})
	if err != nil {
		return fmt.Errorf("marshal addAddressSettings args: %w", err)
	}
	if _, err := c.callManagement(ctx, "broker", "addAddressSettings", string(body)); err != nil {
		return asBrokerTooOld(err)
	}
	return nil
}

// ApplyAddressSettings applies a settings object (JSON) to an address match via
// the two-arg addAddressSettings(String,String) overload. It is the exported
// form of applyAddressSettings for callers outside this package (e.g. the
// journal fixture harvester, which forces paging on one address).
func (c *Client) ApplyAddressSettings(ctx context.Context, match, settingsJSON string) error {
	return c.applyAddressSettings(ctx, match, settingsJSON)
}

// asBrokerTooOld maps the broker's "no operation addAddressSettings" rejection
// (returned by brokers without the two-arg JSON overload, e.g. 2.31.x) to the
// clearer ErrBrokerTooOld, and passes any other error through unchanged.
func asBrokerTooOld(err error) error {
	if err != nil && strings.Contains(err.Error(), "no operation addAddressSettings") {
		return ErrBrokerTooOld
	}
	return err
}

// withCordonPolicy returns the given settings object with only the address-full
// policy and the size thresholds overridden, so every other field (DLA, expiry,
// redelivery, ...) is preserved for the duration of the cordon.
func withCordonPolicy(settingsJSON string) (string, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(settingsJSON), &m); err != nil {
		return "", fmt.Errorf("parse settings object: %w", err)
	}
	m["addressFullMessagePolicy"] = json.RawMessage(`"FAIL"`)
	m["maxSizeBytes"] = json.RawMessage(fmt.Sprintf("%d", cordonMaxSizeBytes))
	m["pageSizeBytes"] = json.RawMessage(fmt.Sprintf("%d", cordonPageSizeBytes))
	m["maxSizeBytesRejectThreshold"] = json.RawMessage(fmt.Sprintf("%d", cordonRejectThreshold))
	out, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("marshal cordon settings: %w", err)
	}
	return string(out), nil
}

// withUncordonPolicy reverses exactly the fields withCordonPolicy overrides,
// resetting the address-full policy and size thresholds to permissive values
// while preserving every other field. It is how UncordonRemove lifts a cordon
// when no saved pre-cordon state is available: on Artemis 2.42
// removeAddressSettings does not reliably clear the wildcard override, so we
// overwrite it with a permissive policy instead.
func withUncordonPolicy(settingsJSON string) (string, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(settingsJSON), &m); err != nil {
		return "", fmt.Errorf("parse settings object: %w", err)
	}
	m["addressFullMessagePolicy"] = json.RawMessage(`"PAGE"`)
	m["maxSizeBytes"] = json.RawMessage("-1")
	m["pageSizeBytes"] = json.RawMessage(fmt.Sprintf("%d", uncordonPageSizeBytes))
	m["maxSizeBytesRejectThreshold"] = json.RawMessage("-1")
	out, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("marshal uncordon settings: %w", err)
	}
	return string(out), nil
}

// Cordon blocks producers across the whole broker by applying an address-full
// BLOCK policy to the wildcard match. It returns the pre-cordon settings JSON so
// the caller can persist it for a later Uncordon. On a broker too old to accept
// the setting over AMQP it returns ErrBrokerTooOld and changes nothing.
func (c *Client) Cordon(ctx context.Context) (savedSettings string, err error) {
	saved, err := c.getWildcardSettings(ctx)
	if err != nil {
		return "", err
	}
	blocked, err := withCordonPolicy(saved)
	if err != nil {
		return "", err
	}
	if err := c.applyAddressSettings(ctx, wildcardMatch, blocked); err != nil {
		return "", err
	}
	return saved, nil
}

// Uncordon restores a previously saved settings JSON to the wildcard match,
// lifting the producer block and faithfully reinstating whatever settings were
// in place before Cordon.
func (c *Client) Uncordon(ctx context.Context, savedSettings string) error {
	return c.applyAddressSettings(ctx, wildcardMatch, savedSettings)
}

// UncordonRemove lifts the cordon when no saved pre-cordon state is available.
// It cannot use removeAddressSettings: on Artemis 2.42 that reports success but
// leaves the wildcard override in place (a no-op), so the cordon never lifts.
// Instead it reads the current wildcard settings and overwrites only the
// address-full policy and size thresholds back to permissive values, undoing
// exactly what Cordon applied while preserving every other field.
func (c *Client) UncordonRemove(ctx context.Context) error {
	current, err := c.getWildcardSettings(ctx)
	if err != nil {
		return err
	}
	permissive, err := withUncordonPolicy(current)
	if err != nil {
		return err
	}
	return c.applyAddressSettings(ctx, wildcardMatch, permissive)
}
