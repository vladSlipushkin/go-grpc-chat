module github.com/vladSlipushkin/go-grpc-chat

go 1.27.0

toolchain go1.27.2

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.12-20260825204119-511051f7f437.2
	buf.build/go/protovalidate v1.4.0
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/google/go-cmp v0.7.0
	github.com/google/uuid v1.6.0
	go.uber.org/goleak v1.3.0
	golang.org/x/crypto v0.58.0
	golang.org/x/text v0.43.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260819154853-08b0e4226688
	// Before upgrading, re-check the grpc-go internals ADR-0009 relies on
	// ("The stream after the handler returns"); no test would notice a change.
	google.golang.org/grpc v1.84.0
	google.golang.org/protobuf v1.36.12
)

require (
	cel.dev/cel-go v0.32.0 // indirect
	cel.dev/expr v0.25.3 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/exp v0.0.0-20260820142414-ca536658362e // indirect
	golang.org/x/net v0.60.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260819154853-08b0e4226688 // indirect
)
