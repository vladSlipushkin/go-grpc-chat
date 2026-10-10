package server

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"

	authv1 "github.com/vladSlipushkin/go-grpc-chat/gen/auth/v1"
)

func TestUnaryValidationRejectsInvalidRequest(t *testing.T) {
	t.Parallel()

	logger, _ := newTestLogger()
	info := &grpc.UnaryServerInfo{FullMethod: "/auth.v1.AuthService/Register"}
	req := &authv1.RegisterRequest{Username: "Al", Password: "short"}

	_, err := unaryValidation(logger)(t.Context(), req, info, func(context.Context, any) (any, error) {
		t.Error("handler called with an invalid request")
		return nil, nil
	})

	st := status.Convert(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("code = %v, want %v (err = %v)", st.Code(), codes.InvalidArgument, err)
	}
	if len(st.Details()) != 1 {
		t.Fatalf("details = %v, want one BadRequest", st.Details())
	}
	br, ok := st.Details()[0].(*errdetails.BadRequest)
	if !ok {
		t.Fatalf("detail = %T, want *errdetails.BadRequest", st.Details()[0])
	}
	var got []string
	for _, v := range br.GetFieldViolations() {
		got = append(got, v.GetField()+":"+v.GetReason())
	}
	want := []string{"username:string.min_len", "username:string.pattern", "password:string.min_len"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("field violations mismatch (-want +got):\n%s", diff)
	}
}

func TestUnaryValidationPassesValidRequest(t *testing.T) {
	t.Parallel()

	logger, _ := newTestLogger()
	info := &grpc.UnaryServerInfo{FullMethod: "/auth.v1.AuthService/Register"}
	req := &authv1.RegisterRequest{Username: "alice", Password: "correct horse"}

	var got any
	_, err := unaryValidation(logger)(t.Context(), req, info, func(_ context.Context, req any) (any, error) {
		got = req
		return nil, nil
	})
	if err != nil {
		t.Fatalf("unaryValidation() error = %v", err)
	}
	if diff := cmp.Diff(req, got, protocmp.Transform()); diff != "" {
		t.Errorf("handler request mismatch (-want +got):\n%s", diff)
	}
}
