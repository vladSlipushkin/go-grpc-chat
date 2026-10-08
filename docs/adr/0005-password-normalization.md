# 5. Password normalization

Date: 2026-10-08

## Status

Accepted

## Context

Passwords may contain any Unicode text. The same visible password can reach
the server as different byte sequences: "é" typed as one precomposed code
point on one system and as "e" plus a combining accent on another, or a
full-width "Ａ" from an East Asian input method instead of "A". bcrypt hashes
bytes, so without normalization the same password would produce different
hashes and a user could be locked out depending on the device they log in
from.

NIST SP 800-63B says verifiers should apply the Normalization Process for
Stabilized Strings using NFKC or NFKD before hashing. RFC 8265 (PRECIS
OpaqueString) uses NFC instead.

bcrypt only reads the first 72 bytes of its input, and the contract already
rejects longer passwords (`auth.proto`). Normalization can change
the byte length, so the limit has to hold for the normalized form.

## Decision

- Passwords are normalized to NFKC (`golang.org/x/text/unicode/norm`) before
  hashing on registration and before comparison on login.
- NFKC over NFKD: both fold compatibility variants such as full-width letters
  and ligatures, but the composed form keeps accented characters as single
  code points, so it uses fewer bytes of the 72-byte budget.
- NFKC over NFC: NFC leaves compatibility variants distinct, so the
  full-width and ligature cases above would still produce different hashes.
  The cost is that a few visually different characters, such as "²" and "2",
  become the same password, which barely affects its strength.
- The 72-byte limit is checked again after normalization. Some characters
  expand under NFKC, so a password that passes the contract check can still
  exceed the limit; it is rejected with `INVALID_ARGUMENT` on the `password`
  field. Without the check, Go's bcrypt would return `ErrPasswordTooLong`
  and the client would see an internal error.
- ASCII passwords are unchanged by NFKC.
- Unassigned code points are not rejected, although a strict stabilized
  string would exclude them. A password using one could normalize differently
  after a Unicode version upgrade; the risk is accepted as negligible.

## Consequences

- A password works the same from any device or input method.
- Normalization has to be in place before the first account is created:
  adding it later would make existing hashes unreachable for non-ASCII
  passwords until every affected user logs in once more and is re-hashed.
- This is the first item to cut if the schedule slips. If it is cut, that has
  to happen before any account exists, and this decision is then superseded.
