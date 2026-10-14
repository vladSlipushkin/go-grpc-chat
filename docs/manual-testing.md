# Manual testing on Windows

How to try the server by hand: the chat client in two terminals, then the
raw API with grpcurl. Every command below works as written in both cmd and
PowerShell. Run them from the repository root.

Requirements: Go 1.27 and GNU make. Docker is not needed.

## 1. Start the server

```
make run
```

`make run` turns on reflection and uses a fixed development JWT secret. The
server listens on `localhost:50051` and logs JSON to the terminal; stop it
with Ctrl+C.

Accounts and rooms live in memory: a restart forgets every account, so
register again after one.

To try token expiry without waiting an hour, shorten the token lifetime:

```
make run CHAT_TOKEN_TTL=1m
```

## 2. Build the binaries

In a second terminal:

```
make build
go build -modfile tools/go.mod -o bin/grpcurl.exe github.com/fullstorydev/grpcurl/cmd/grpcurl
```

This puts `chat.exe`, `server.exe` and `grpcurl.exe` in `bin\`. grpcurl is
pinned in `tools/go.mod` like the other development tools.

## 3. Two users in two terminals

Register both users, in any terminal. Each command asks for the password
twice; nothing is shown while you type it.

```
bin\chat.exe register -user alice
bin\chat.exe register -user bob
```

Terminal A:

```
bin\chat.exe -user alice
```

Terminal B:

```
bin\chat.exe -user bob
```

Then type in A and B:

| Terminal | Type | Expect |
|---|---|---|
| A | `/join general` | `-- joined general --` |
| B | `/join general` | `-- joined general --` |
| A | `hello` | `[general] alice: hello` in both terminals |
| B | `/leave` | `-- left general --` |
| A | `still there?` | the message in A only |
| A | `/rooms` | `-- rooms: general (current) --` |
| A, B | `/quit` | back to the shell prompt |

`/help` lists the commands. Ctrl+C quits like `/quit`.

Things worth checking on the way:

- **Reconnect.** Stop the server with Ctrl+C while a client is in a room.
  The client prints `-- connection lost (server unavailable); reconnecting
  in ... --` with a growing delay. Start `make run` again: the client prints
  `-- reconnected; rejoined general --`. Messages sent while it was away are
  not shown.
- **Token expiry.** With `make run CHAT_TOKEN_TTL=1m`, a client prints
  `-- session expired; press Enter to log in again --` a minute after
  logging in. Press Enter, type the password, and it rejoins its rooms. The
  account must still exist, so this does not survive a server restart.
- **Plaintext to another host.** `bin\chat.exe -user alice -addr
  192.168.1.10:50051` refuses to connect: without TLS the password would
  cross the network in the clear. `-insecure` overrides it.

## 4. The API with grpcurl

grpcurl reads requests from standard input with `-d "@"`; the quotes keep
PowerShell from treating `@` as an operator. Type or paste the JSON, press
Enter, and end the input with Ctrl+Z followed by Enter. Typing the requests
avoids quoting JSON on the command line, which cmd and PowerShell handle
differently.

Health:

```
bin\grpcurl.exe -plaintext localhost:50051 grpc.health.v1.Health/Check
```

```
{
  "status": "SERVING"
}
```

Register:

```
bin\grpcurl.exe -plaintext -d "@" localhost:50051 auth.v1.AuthService/Register
```

```
{"username": "carol", "password": "secret-pass-1"}
```

Ctrl+Z, Enter. The reply carries the new `userId`.

Log in the same way:

```
bin\grpcurl.exe -plaintext -d "@" localhost:50051 auth.v1.AuthService/Login
```

```
{"username": "carol", "password": "secret-pass-1"}
```

Ctrl+Z, Enter. Copy the `accessToken` from the reply.

Open the chat stream, with the token in place of `TOKEN`:

```
bin\grpcurl.exe -plaintext -H "authorization: Bearer TOKEN" -d "@" localhost:50051 chat.v1.ChatService/Connect
```

The stream stays open. Type one request per line, each with its own
`request_id`:

```
{"request_id": "1", "join_room": {"room_id": "general"}}
{"request_id": "2", "send_message": {"room_id": "general", "text": "hello"}}
{"request_id": "3", "leave_room": {"room_id": "general"}}
```

Each request gets an `ack` with its `requestId`; the sent message also comes
back as a `message` with its `seq`. A second grpcurl or a chat client in the
same room sees it too. Ctrl+Z, Enter closes the stream.

Wait for the replies before Ctrl+Z. The server ends the stream as soon as
the client closes its side, and replies it has not sent by then are lost
(ADR-0009). For the same reason, piping a file of requests into grpcurl
applies them but may print no replies.

Other services are listed by reflection:

```
bin\grpcurl.exe -plaintext localhost:50051 list
```
