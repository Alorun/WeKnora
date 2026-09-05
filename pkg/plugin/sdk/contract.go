// Package sdk is the public, WeKnora-internal-free Go surface for V1 plugins.
package sdk

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	ProtocolVersion           = "1.0"
	DataSourceContractVersion = "1.0"

	MaxConfigBytes   = 256 << 10
	MaxCursorBytes   = 1 << 20
	MaxDocumentBytes = 32 << 20
	MaxMessageBytes  = MaxDocumentBytes + (1 << 20)
)

const (
	ErrorInvalidArgument        = "invalid_argument"
	ErrorIdentityMismatch       = "identity_mismatch"
	ErrorNonceMismatch          = "nonce_mismatch"
	ErrorIncompatibleProtocol   = "incompatible_protocol"
	ErrorIncompatibleContract   = "incompatible_contract"
	ErrorUnsupportedCapability  = "unsupported_capability"
	ErrorMessageTooLarge        = "message_too_large"
	ErrorNotReady               = "not_ready"
	ErrorUnavailable            = "unavailable"
	ErrorInternal               = "internal"
	ErrorRuntimeNotAvailable    = "runtime_not_available"
	ErrorPluginStreamItemFailed = "plugin_stream_item_failed"
)

var ErrIncompatibleVersion = errors.New("incompatible contract version")

// CompatibleVersion accepts the same major and an implementation minor that
// is at least the requested minor. Patch components are deliberately ignored:
// the wire contract evolves only at major/minor granularity.
func CompatibleVersion(requested, implemented string) bool {
	rMajor, rMinor, err := versionParts(requested)
	if err != nil {
		return false
	}
	iMajor, iMinor, err := versionParts(implemented)
	return err == nil && rMajor == iMajor && iMinor >= rMinor
}

func versionParts(version string) (int, int, error) {
	parts := strings.Split(version, ".")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, 0, fmt.Errorf("version %q must be major.minor[.patch]", version)
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil || major < 0 {
		return 0, 0, fmt.Errorf("invalid major version %q", version)
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil || minor < 0 {
		return 0, 0, fmt.Errorf("invalid minor version %q", version)
	}
	if len(parts) == 3 {
		patch, patchErr := strconv.Atoi(parts[2])
		if patchErr != nil || patch < 0 {
			return 0, 0, fmt.Errorf("invalid patch version %q", version)
		}
	}
	return major, minor, nil
}
