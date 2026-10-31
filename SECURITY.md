# Security policy

## Supported versions

Only the [latest release](https://github.com/Clawdlinux/atticdb/releases/latest) gets security fixes.

## Report a vulnerability

Report it privately through the [private vulnerability reporting form](https://github.com/Clawdlinux/atticdb/security/advisories/new).
Don't open a public issue, pull request or discussion for a vulnerability.
The form is the only private channel. There is no email contact.

Include these:

- the AtticDB version, from `atticdb --version`;
- your OS and its version;
- the steps to reproduce it;
- the impact: what an attacker can read, write or break.

## Scope

- AtticDB is a local server. TCP listens on loopback only. A non-loopback address fails startup, as AtticDB has no TLS.
- The token in the config file grants its whole tenant. On Unix, the config and any token file must be yours, with no group or other bits. On Windows they must be inside the user profile, and their access lists aren't checked (D92).
- The data directory must be mode 0700 (D75). On Windows it must be inside the user profile instead (D92). Anyone with write access to it can edit history.
