package sync

import (
	"regexp"
	"strings"
	"unicode/utf16"

	syncv1 "github.com/Luminet2023/hifumi-backend/api/sync/v1"
)

const (
	maxSafeInteger       = uint64(1<<53 - 1)
	maxMutations         = 200
	maxResolutionRecords = 768
	maxValueJSONBytes    = 128 * 1024
	defaultPullLimit     = uint32(128)
	maxPullLimit         = uint32(256)
)

var (
	baselineIDPattern = regexp.MustCompile(`^baseline_[a-f0-9]{32}$`)
	deviceIDPattern   = regexp.MustCompile(`^[A-Za-z0-9_-]{8,128}$`)
	requestIDPattern  = regexp.MustCompile(`^[A-Za-z0-9_-]{12,128}$`)
	ownerKeyPattern   = regexp.MustCompile(`^[a-f0-9]{64}$`)
)

func validateOwnerKey(ownerKey string) error {
	if !ownerKeyPattern.MatchString(ownerKey) {
		return invalidArgument("invalid owner_key")
	}
	return nil
}

func validateBaselineID(value, label string) error {
	if !baselineIDPattern.MatchString(value) {
		return invalidArgument("invalid " + label)
	}
	return nil
}

func validateDeviceID(value string) error {
	if !deviceIDPattern.MatchString(value) {
		return invalidArgument("invalid device_id")
	}
	return nil
}

func validateSafe(values ...uint64) error {
	for _, value := range values {
		if value > maxSafeInteger {
			return invalidArgument("protobuf uint64 exceeds JavaScript safe integer range")
		}
	}
	return nil
}

func validateMutation(mutation *syncv1.Mutation, requestDeviceID string) error {
	if mutation == nil || !deviceIDPattern.MatchString(mutation.GetOpId()) {
		return invalidArgument("invalid mutation op_id")
	}
	if !strings.HasPrefix(mutation.GetEntityKey(), "stella/v1/") || utf16Length(mutation.GetEntityKey()) > 512 {
		return invalidArgument("invalid mutation entity_key")
	}
	if mutation.GetDeviceId() != requestDeviceID {
		return invalidArgument("mutation device_id mismatch")
	}
	if len(mutation.GetValueJson()) > maxValueJSONBytes {
		return invalidArgument("mutation value is too large")
	}
	return validateSafe(mutation.GetBaseVersion(), mutation.GetClientTimeMs(), mutation.GetClientSeq())
}

func validateExchangeRequest(ownerKey string, request *syncv1.SyncRequest) error {
	if request == nil {
		return invalidArgument("missing sync request")
	}
	if err := validateOwnerKey(ownerKey); err != nil {
		return err
	}
	if err := validateDeviceID(request.GetDeviceId()); err != nil {
		return err
	}
	if err := validateBaselineID(request.GetBaselineId(), "baseline_id"); err != nil {
		return err
	}
	if len(request.GetMutations()) > maxMutations {
		return invalidArgument("too many mutations")
	}
	if err := validateSafe(request.GetCursor(), request.GetLocalVersion(), request.GetLocalUpdatedAtMs()); err != nil {
		return err
	}
	for _, mutation := range request.GetMutations() {
		if err := validateMutationWireNumbers(mutation); err != nil {
			return err
		}
	}
	return nil
}

// ValidateExchange 在进入 Redis 限流前校验请求，以保持旧 Worker 对无效请求
// 不消耗限流额度的行为。Service 仍会在事务前再次校验。
func ValidateExchange(ownerKey string, request *syncv1.SyncRequest) error {
	return validateExchangeRequest(ownerKey, request)
}

func validateDiffRequest(ownerKey string, request *syncv1.DiffRequest) error {
	if request == nil {
		return invalidArgument("missing diff request")
	}
	if err := validateOwnerKey(ownerKey); err != nil {
		return err
	}
	if err := validateDeviceID(request.GetDeviceId()); err != nil {
		return err
	}
	if err := validateBaselineID(request.GetBaselineId(), "baseline_id"); err != nil {
		return err
	}
	if len(request.GetMutations()) > maxMutations {
		return invalidArgument("too many mutations")
	}
	if err := validateSafe(request.GetLocalVersion(), request.GetLocalUpdatedAtMs()); err != nil {
		return err
	}
	for _, mutation := range request.GetMutations() {
		if err := validateMutationWireNumbers(mutation); err != nil {
			return err
		}
	}
	return nil
}

// ValidateDiff 在进入 Redis 限流前校验请求。Service 仍会在事务前再次校验。
func ValidateDiff(ownerKey string, request *syncv1.DiffRequest) error {
	return validateDiffRequest(ownerKey, request)
}

func validateResolveEnvelope(ownerKey string, request *syncv1.ResolveBaselineRequest) error {
	if request == nil {
		return invalidArgument("missing baseline resolution request")
	}
	if err := validateOwnerKey(ownerKey); err != nil {
		return err
	}
	if err := validateDeviceID(request.GetDeviceId()); err != nil {
		return err
	}
	if err := validateBaselineID(request.GetLocalBaselineId(), "local_baseline_id"); err != nil {
		return err
	}
	if err := validateBaselineID(request.GetExpectedServerBaselineId(), "expected_server_baseline_id"); err != nil {
		return err
	}
	if !requestIDPattern.MatchString(request.GetRequestId()) {
		return invalidArgument("invalid request_id")
	}
	if request.GetChoice() != syncv1.BaselineChoice_BASELINE_CHOICE_USE_LOCAL &&
		request.GetChoice() != syncv1.BaselineChoice_BASELINE_CHOICE_USE_SERVER {
		return invalidArgument("invalid baseline choice")
	}
	if len(request.GetLocalSnapshot()) > maxResolutionRecords {
		return invalidArgument("snapshot has too many records")
	}
	if err := validateSafe(
		request.GetExpectedServerVersion(),
		request.GetLocalVersion(),
		request.GetLocalUpdatedAtMs(),
	); err != nil {
		return err
	}
	for _, mutation := range request.GetLocalSnapshot() {
		if err := validateMutationWireNumbers(mutation); err != nil {
			return err
		}
	}
	return nil
}

func ValidateResolve(ownerKey string, request *syncv1.ResolveBaselineRequest) error {
	return validateResolveEnvelope(ownerKey, request)
}

func validateMutationWireNumbers(mutation *syncv1.Mutation) error {
	if mutation == nil {
		return nil
	}
	return validateSafe(mutation.GetBaseVersion(), mutation.GetClientTimeMs(), mutation.GetClientSeq())
}

func validateLocalSnapshot(request *syncv1.ResolveBaselineRequest) error {
	seen := make(map[string]struct{}, len(request.GetLocalSnapshot()))
	for _, mutation := range request.GetLocalSnapshot() {
		if err := validateMutation(mutation, request.GetDeviceId()); err != nil {
			return err
		}
		if _, exists := seen[mutation.GetEntityKey()]; exists {
			return invalidArgument("snapshot contains duplicate entity_key")
		}
		seen[mutation.GetEntityKey()] = struct{}{}
	}
	return nil
}

func normalizedPullLimit(value uint32) int {
	if value == 0 {
		value = defaultPullLimit
	}
	if value > maxPullLimit {
		value = maxPullLimit
	}
	if value < 1 {
		value = 1
	}
	return int(value)
}

func utf16Length(value string) int {
	return len(utf16.Encode([]rune(value)))
}
