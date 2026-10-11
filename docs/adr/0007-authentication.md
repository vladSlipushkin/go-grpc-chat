# 7. Authentication

Date: 2026-10-10

## Status

Accepted

## Context

Every chat call has to know which user makes it: the sender of a message is
taken from the access token, never from the request (ADR-0003). Users
register with a username and password and exchange them for a short-lived
access token. The chat stream lives for up to the token's lifetime, so the
token is checked once when the stream opens and must still end the stream
when it expires. Passwords are normalized before hashing (ADR-0005).

## Decision

### Passwords

- bcrypt with cost 12, about a quarter of a second per hash on a current
  server core. The cost is stored in each hash, so raising it later only
  affects new hashes. Tests lower it to the minimum through
  `auth.Options.BcryptCost`.
- A login with an unknown username compares the password against a dummy
  hash and returns the same `UNAUTHENTICATED` status and message as a wrong
  password, so neither the response nor its timing tells whether the user
  exists. The dummy hash is generated at startup with the configured cost,
  so it costs as much as the hashes of users registered under that cost.
- `Register` and `Login` check the context before hashing and return its
  error if the caller has already gone away; bcrypt itself cannot be
  interrupted. Such calls end with `CANCELLED` or `DEADLINE_EXCEEDED` and
  are not logged as failures.
- A password longer than 72 bytes after normalization never matches on
  login, even if its first 72 bytes are a registered password: no stored
  hash was made from it, and truncating would accept a different password.

### Users

`auth.UserStore` is declared in `auth`, where it is used. The in-memory
implementation (`internal/memory`) checks for a taken username and inserts
under one lock, so of concurrent registrations of one name exactly one
succeeds. It copies password hashes in and out, so no caller shares a
slice with the store. PostgreSQL will implement the same interface with a
unique constraint, mapping a unique violation to `ErrUsernameTaken` and a
missing row to `ErrUserNotFound`; any other error is internal and never
takes the dummy-hash path. User ids are UUIDv7: created without coordination, opaque to
clients, and time-ordered, so they index well in a B-tree once stored.

Accounts kept in memory are lost on restart until storage exists.

### Access tokens

- JWT signed with HS256 using `github.com/golang-jwt/jwt/v5`. The server
  that issues tokens is the only one that verifies them, so a shared secret
  is enough; an asymmetric algorithm would only add key management.
- The algorithm is pinned: the parser accepts HS256 only, so `"alg": "none"`
  and algorithm-confusion tokens are rejected before the signature is
  checked.
- Claims: `sub` (user id), `username`, `iat`, `exp`. `exp` is required.
- Lifetime `CHAT_TOKEN_TTL`, one hour by default (ADR-0003 explains the
  trade-off with reconnects).
- The secret comes from `CHAT_JWT_SECRET`, which has no default and must be
  at least 32 bytes, the HS256 output size (RFC 7518, section 3.2). A
  built-in default would let anyone mint tokens for a deployment that forgot
  to set one. `make run` sets a fixed, public development value.
- Expiry is checked exactly: a token is invalid from the second of its
  `exp` on, and a stream's deadline is that same instant. Issue times are
  truncated to whole seconds so the expiry returned by `Login` equals the
  one in the token. With several instances a small leeway would apply only
  to `iat` and `nbf` (ADR-0003).

### Interceptors

The unary chain is logging, recovery, auth, validation; the stream chain is
logging, recovery, auth. The first interceptor is the outermost.

- Logging is outermost so rejected calls are logged with the status the
  client receives.
- Auth requires a valid `authorization: Bearer <token>` header on every
  method except an explicit list of public methods, given by full name:
  `Register` and `Login` of `auth.v1.AuthService`, the health checking
  methods and reflection. Public is opt-in per method, so a new service and
  a new method of a public service, such as a future `ChangePassword`, are
  both protected by default. Listing whole services would have made such a
  method public by accident.
- Validation runs after auth, so callers without a token learn nothing
  about the request rules of protected methods. Violations are returned as
  `INVALID_ARGUMENT` with a `google.rpc.BadRequest` listing every field.
  The 72-byte limit after normalization is reported the same way.
- The verified claims reach handlers through `auth.NewContext` and
  `auth.FromContext`.
- For a stream, auth replaces the stream's context with one that carries the
  claims and ends at the token's expiry:
  `context.WithDeadlineCause(ctx, exp, errTokenExpired)`. When the cause is
  `errTokenExpired` and the handler returned nothing more specific than
  that its context ended (nil, `context.DeadlineExceeded` or its
  `DEADLINE_EXCEEDED` status), the stream ends with `UNAUTHENTICATED` and
  reason `TOKEN_EXPIRED`. Any other handler error, such as `INTERNAL`, is
  returned as is, because it describes what actually went wrong. A client
  cancelling or its own shorter deadline keep their own status.
- Expiry cancels the context only; a `RecvMsg` or `SendMsg` blocked on the
  transport does not return until the handler does. Stream handlers must
  watch the context in their main goroutine and return when it ends,
  without first waiting for a reader blocked in `RecvMsg`, which is released
  once the handler returns. The chat stream handler, not yet written, is to be
  tested for this with a client that stays silent past its token's expiry.
- An expired token on a unary call carries the same `TOKEN_EXPIRED` reason.
- Logging puts a small holder in the context that auth fills with the user
  id, so the record written when the call ends includes `user_id`. Logging
  cannot read the claims directly: auth adds them to a context that only
  flows inward. Only interceptors write to the holder, before the handler
  is called; nothing may write to it from the handler's goroutines.
- Nothing logs request bodies or metadata. A test runs registration, login
  and protected calls, both successful and failing, and checks that the
  password, the token and the `authorization` header never reach the log.

## Accepted risks

- `Register` answers `ALREADY_EXISTS` for a taken username, which tells
  anyone whether a name is registered. Usernames are public anyway: they are
  shown as the sender of every message.
- Both `AuthService` methods are public and each costs a bcrypt hash, so a
  client can spend the server's CPU at a quarter of a second per request.
  Nothing bounds how many hashes run at once; a semaphore on bcrypt is
  planned together with rate limiting.
- Raising the cost breaks the timing equivalence for existing users: the
  dummy hash uses the new cost, older hashes the old one, so an unknown
  username answers measurably slower than a registered one until those
  users log in again. The fix is rehash-on-login (when
  `bcrypt.Cost(hash)` is below the configured cost, store a new hash after a
  successful login) together with a dummy at the old cost during the
  migration. It is not built: the cost has never changed.
- The secret's length is checked, not its entropy: 32 repeated characters
  pass. Operators are expected to generate it, for example with
  `openssl rand -base64 32`.

## Non-goals

- Rate limiting of logins and registrations; it belongs in front of the
  server or in a later interceptor.
- Refresh tokens and token revocation (ADR-0003).
- Password rules beyond the length limits in the contract.

## Consequences

- Handlers get the caller from the context and never parse tokens.
- Every deployment has to provide its own secret; the server refuses to
  start without one.
- Rotating the secret invalidates every issued token; clients log in again.
  Rotation without that would need a `kid` header and two keys accepted
  during the switch.
- Adding a service or a method makes it protected unless its full method
  name is added to the public list in `cmd/server`.
