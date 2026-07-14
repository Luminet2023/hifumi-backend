package sync

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"

	syncv1 "github.com/Luminet2023/hifumi-backend/api/sync/v1"
)

const campaignStart = "2026-07-13"

func deriveProgressDay(records []*syncv1.Change) (string, error) {
	dates := make([]string, 0)
	for _, record := range records {
		if record == nil || record.GetDeleted() {
			continue
		}
		parts := strings.Split(record.GetEntityKey(), "/")
		if len(parts) < 3 {
			continue
		}
		segments := make([]string, 0, len(parts)-2)
		for _, part := range parts[2:] {
			decoded, err := url.PathUnescape(part)
			if err != nil {
				return "", err
			}
			segments = append(segments, decoded)
		}

		var value any
		validJSON := json.Unmarshal(record.GetValueJson(), &value) == nil
		if !validJSON {
			value = nil
		}

		switch {
		case len(segments) >= 3 && segments[0] == "day" && segments[1] != "":
			kind := segments[2]
			if meaningfulDayRecord(kind, value, validJSON) {
				dates = append(dates, segments[1])
			}
		case len(segments) >= 1 && segments[0] == "quote":
			if date := objectString(value, "date"); date != "" {
				dates = append(dates, date)
			}
		case len(segments) >= 1 && segments[0] == "raffle":
			date := objectString(value, "drawDate")
			if date == "" {
				date = objectString(value, "date")
			}
			if date != "" {
				dates = append(dates, date)
			}
		}
	}
	if len(dates) == 0 {
		return campaignStart, nil
	}
	sort.Strings(dates)
	return dates[len(dates)-1], nil
}

func meaningfulDayRecord(kind string, value any, validJSON bool) bool {
	switch kind {
	case "journal":
		return strings.TrimSpace(jsString(value)) != ""
	case "goals":
		return jsTruthy(objectValue(value, "lockedAt"))
	case "blessing":
		return jsTruthy(objectValue(value, "liked"))
	case "item":
		if strings.TrimSpace(jsString(objectValue(value, "input"))) != "" {
			return true
		}
		// JS 的 undefined !== "pending" 为 true；保留旧实现对无效 JSON 的行为。
		if !validJSON {
			return true
		}
		return jsString(objectValue(value, "status")) != "pending"
	default:
		return false
	}
}

func objectValue(value any, key string) any {
	object, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	return object[key]
}

func objectString(value any, key string) string {
	return jsString(objectValue(value, key))
}

func jsString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case bool:
		if typed {
			return "true"
		}
		return "false"
	case float64:
		return fmt.Sprintf("%v", typed)
	case map[string]any:
		return "[object Object]"
	case []any:
		parts := make([]string, len(typed))
		for index, item := range typed {
			parts[index] = jsString(item)
		}
		return strings.Join(parts, ",")
	default:
		return fmt.Sprint(typed)
	}
}

func jsTruthy(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case string:
		return typed != ""
	case float64:
		return typed != 0
	default:
		return true
	}
}
