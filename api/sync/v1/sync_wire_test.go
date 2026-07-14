package syncv1

import (
	"encoding/hex"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestJavaScriptWireGoldenFixtures(t *testing.T) {
	mutation := &Mutation{
		OpId:         "o",
		EntityKey:    "e",
		BaseVersion:  1,
		ClientTimeMs: 300,
		ValueJson:    []byte{0, 0xff},
		Deleted:      true,
		DeviceId:     "d",
		ClientSeq:    2,
	}
	ack := &MutationAck{OpId: "o", ServerCursor: 1, Conflict: true, Applied: true}
	change := &Change{
		Cursor:       1,
		EntityKey:    "e",
		ValueJson:    []byte{0, 0xff},
		Deleted:      true,
		DeviceId:     "d",
		ClientTimeMs: 300,
		OpId:         "o",
	}

	tests := []struct {
		name    string
		message proto.Message
		hex     string
		fresh   func() proto.Message
	}{
		{
			name:    "mutation",
			message: mutation,
			hex:     "0a016f120165180120ac022a0200ff30013a01644002",
			fresh:   func() proto.Message { return &Mutation{} },
		},
		{
			name:    "mutation ack",
			message: ack,
			hex:     "0a016f100118012001",
			fresh:   func() proto.Message { return &MutationAck{} },
		},
		{
			name:    "change",
			message: change,
			hex:     "08011201651a0200ff20012a016430ac023a016f",
			fresh:   func() proto.Message { return &Change{} },
		},
		{
			name: "sync request",
			message: &SyncRequest{
				DeviceId: "d", Cursor: 1, Mutations: []*Mutation{mutation}, PullLimit: 128,
				BaselineId: "b", LocalVersion: 2, LocalUpdatedAtMs: 300, LocalProgressDay: "p",
			},
			hex:   "0a016410011a160a016f120165180120ac022a0200ff30013a016440022080012a0162300238ac02420170",
			fresh: func() proto.Message { return &SyncRequest{} },
		},
		{
			name: "sync response",
			message: &SyncResponse{
				NextCursor: 1, Acks: []*MutationAck{ack}, Changes: []*Change{change}, HasMore: true,
				ResetRequired: true, BaselineId: "b", ServerVersion: 2, ServerUpdatedAtMs: 300,
				ServerProgressDay: "p", BaselineMismatch: true,
			},
			hex:   "080112090a016f1001180120011a1408011201651a0200ff20012a016430ac023a016f20012801320162380240ac024a01705001",
			fresh: func() proto.Message { return &SyncResponse{} },
		},
		{
			name: "resolve request",
			message: &ResolveBaselineRequest{
				RequestId: "r", DeviceId: "d", LocalBaselineId: "l", ExpectedServerBaselineId: "s",
				ExpectedServerVersion: 2, Choice: BaselineChoice_BASELINE_CHOICE_USE_LOCAL,
				LocalSnapshot: []*Mutation{mutation}, LocalVersion: 3, LocalUpdatedAtMs: 300,
				LocalProgressDay: "p",
			},
			hex:   "0a01721201641a016c220173280230013a160a016f120165180120ac022a0200ff30013a01644002400348ac02520170",
			fresh: func() proto.Message { return &ResolveBaselineRequest{} },
		},
		{
			name: "resolve response",
			message: &ResolveBaselineResponse{
				BaselineId: "b", ServerVersion: 2, ServerUpdatedAtMs: 300, ServerProgressDay: "p",
				Records: []*Change{change}, Stale: true, ServerCursor: 3,
			},
			hex:   "0a0162100218ac022201702a1408011201651a0200ff20012a016430ac023a016f30013803",
			fresh: func() proto.Message { return &ResolveBaselineResponse{} },
		},
	}

	marshal := proto.MarshalOptions{Deterministic: true}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want, err := hex.DecodeString(test.hex)
			if err != nil {
				t.Fatal(err)
			}
			got, err := marshal.Marshal(test.message)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != string(want) {
				t.Fatalf("wire mismatch\n got: %x\nwant: %x", got, want)
			}
			decoded := test.fresh()
			if err := proto.Unmarshal(want, decoded); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(decoded, test.message) {
				t.Fatalf("decoded message mismatch\n got: %v\nwant: %v", decoded, test.message)
			}
		})
	}
}

func TestDefaultMessagesEncodeToEmptyBytes(t *testing.T) {
	messages := []proto.Message{
		&Mutation{}, &MutationAck{}, &Change{}, &SyncRequest{}, &SyncResponse{},
		&ResolveBaselineRequest{}, &ResolveBaselineResponse{},
	}
	for _, message := range messages {
		encoded, err := proto.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) != 0 {
			t.Fatalf("%T encoded non-default bytes: %x", message, encoded)
		}
	}
}
