// Package api_test checks the validation rules declared in the protobuf
// contracts, so a rule change shows up as a failing test rather than as a
// behaviour change noticed in production.
package api_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"

	authv1 "github.com/vladSlipushkin/go-grpc-chat/gen/auth/v1"
	chatv1 "github.com/vladSlipushkin/go-grpc-chat/gen/chat/v1"
)

func TestAuthContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		msg  proto.Message
		want []string
	}{
		{
			name: "valid registration",
			msg:  &authv1.RegisterRequest{Username: "alice_01", Password: "correct horse"},
		},
		{
			name: "username too short",
			msg:  &authv1.RegisterRequest{Username: "al", Password: "correct horse"},
			want: []string{"username:string.min_len"},
		},
		{
			name: "username with space",
			msg:  &authv1.RegisterRequest{Username: "alice smith", Password: "correct horse"},
			want: []string{"username:string.pattern"},
		},
		{
			name: "username with uppercase",
			msg:  &authv1.RegisterRequest{Username: "Alice", Password: "correct horse"},
			want: []string{"username:string.pattern"},
		},
		{
			name: "password too short",
			msg:  &authv1.RegisterRequest{Username: "alice", Password: "1234567"},
			want: []string{"password:string.min_len"},
		},
		{
			name: "password at bcrypt limit",
			msg:  &authv1.RegisterRequest{Username: "alice", Password: strings.Repeat("a", 72)},
		},
		{
			name: "password over bcrypt limit",
			msg:  &authv1.RegisterRequest{Username: "alice", Password: strings.Repeat("a", 73)},
			want: []string{"password:string.max_bytes"},
		},
		{
			// 36 Cyrillic letters are 72 bytes in UTF-8.
			name: "multibyte password at bcrypt limit",
			msg:  &authv1.RegisterRequest{Username: "alice", Password: strings.Repeat("ж", 36)},
		},
		{
			// Only 37 characters, but 74 bytes: the limit is in bytes.
			name: "multibyte password over bcrypt limit",
			msg:  &authv1.RegisterRequest{Username: "alice", Password: strings.Repeat("ж", 37)},
			want: []string{"password:string.max_bytes"},
		},
		{
			name: "login without credentials",
			msg:  &authv1.LoginRequest{},
			want: []string{"password:required", "username:required"},
		},
		{
			name: "login username over length limit",
			msg:  &authv1.LoginRequest{Username: strings.Repeat("a", 33), Password: "correct horse"},
			want: []string{"username:string.max_len"},
		},
		{
			name: "login password over bcrypt limit",
			msg:  &authv1.LoginRequest{Username: "alice", Password: strings.Repeat("a", 73)},
			want: []string{"password:string.max_bytes"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if diff := cmp.Diff(tt.want, violations(t, tt.msg)); diff != "" {
				t.Errorf("violations mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestChatContract(t *testing.T) {
	t.Parallel()

	join := func(room string) *chatv1.ConnectRequest {
		return &chatv1.ConnectRequest{
			RequestId: "r1",
			Action:    &chatv1.ConnectRequest_JoinRoom{JoinRoom: &chatv1.JoinRoom{RoomId: room}},
		}
	}
	send := func(text string) *chatv1.ConnectRequest {
		return &chatv1.ConnectRequest{
			RequestId: "r1",
			Action: &chatv1.ConnectRequest_SendMessage{
				SendMessage: &chatv1.SendMessage{RoomId: "general", Text: text},
			},
		}
	}

	tests := []struct {
		name string
		msg  proto.Message
		want []string
	}{
		{
			name: "valid join",
			msg:  join("general"),
		},
		{
			name: "action is required",
			msg:  &chatv1.ConnectRequest{RequestId: "r1"},
			want: []string{"action:required"},
		},
		{
			name: "request id is required",
			msg: &chatv1.ConnectRequest{
				Action: &chatv1.ConnectRequest_LeaveRoom{LeaveRoom: &chatv1.LeaveRoom{RoomId: "general"}},
			},
			want: []string{"request_id:string.min_len"},
		},
		{
			name: "room id must be lowercase",
			msg:  join("General"),
			want: []string{"join_room.room_id:string.pattern"},
		},
		{
			name: "room id at length limit",
			msg:  join(strings.Repeat("a", 64)),
		},
		{
			name: "room id over length limit",
			msg:  join(strings.Repeat("a", 65)),
			want: []string{"join_room.room_id:string.max_len"},
		},
		{
			name: "empty room id",
			msg:  join(""),
			want: []string{"join_room.room_id:string.pattern"},
		},
		{
			name: "empty message text",
			msg:  send(""),
			want: []string{"send_message.text:string.min_len"},
		},
		{
			name: "message text at byte limit",
			msg:  send(strings.Repeat("a", 4096)),
		},
		{
			name: "message text over byte limit",
			msg:  send(strings.Repeat("a", 4097)),
			want: []string{"send_message.text:string.max_bytes"},
		},
		{
			// 2048 Cyrillic letters are 4096 bytes in UTF-8.
			name: "multibyte message text at byte limit",
			msg:  send(strings.Repeat("ж", 2048)),
		},
		{
			name: "multibyte message text over byte limit",
			msg:  send(strings.Repeat("ж", 2049)),
			want: []string{"send_message.text:string.max_bytes"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if diff := cmp.Diff(tt.want, violations(t, tt.msg)); diff != "" {
				t.Errorf("violations mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// violations returns the sorted "field:rule" pairs reported for msg, or nil
// when msg is valid.
func violations(t *testing.T, msg proto.Message) []string {
	t.Helper()

	err := protovalidate.Validate(msg)
	if err == nil {
		return nil
	}
	var verr *protovalidate.ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("Validate(%v) = %v, want a *protovalidate.ValidationError", msg, err)
	}

	got := make([]string, 0, len(verr.Violations))
	for _, v := range verr.Violations {
		got = append(got, protovalidate.FieldPathString(v.Proto.GetField())+":"+v.Proto.GetRuleId())
	}
	slices.Sort(got)
	return got
}
