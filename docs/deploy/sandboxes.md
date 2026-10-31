# Agents in sandboxes and microVMs

Run `atticdb serve` on the host or in a sidecar. Never run it inside a disposable sandbox.
By default the harness outside the sandbox holds memory, and sandboxed code gets no access (D79).
The harness is the trusted loop that calls the model and starts sandboxes. It reads the window and facts, builds the prompt and appends results.
Code in a microVM or container gets no token and no socket, because a token grants a whole tenant.
This guide is for agents inside sandboxes that reach it through its Unix socket.
A token grants its whole tenant. Use these recipes only for code you trust with that tenant, or give each sandbox a tenant of its own.
There is no container image yet.

## Give the socket its own directory

The default socket is `<data_dir>/atticdb.sock`.
Do not mount the data directory into a sandbox. It holds the tape, indexes, `LOCK` and `serve.log`.
Anyone who can read those files skips token auth.
Put the socket in a directory of its own and share only that directory:

```sh
install -d -o atticdb -g agents -m 0750 /run/atticdb
atticdb serve --config /etc/atticdb/atticdb.json --data-dir /var/lib/atticdb \
  --listen none --unix-socket /run/atticdb/atticdb.sock --unix-socket-group agents
```

The socket's directory must be owned by the server's user and not writable by group or others.
The socket is mode 0600. With `--unix-socket-group` it is 0660 with that group.
A client process needs the server's uid, or membership in that group.
Give sandboxes their own uid and the socket group, not the server's uid.
Group names come from `/etc/group`. For LDAP or other NSS groups, pass the numeric gid.

Share the directory, never the socket file.
A restarted server installs a new socket file at the same path.
A hard link or a file bind mount keeps pointing at the old, dead one.

Mount the directory read-only in every sandbox.
With a writable mount and the server's uid, a sandbox process can unlink the socket and bind its own.
It can then copy the public instance id and collect other clients' tokens.
A read-only mount still allows `connect(2)`, and the host still replaces the socket on restart.

## Firecracker

A guest connects with `AF_VSOCK` to CID 2, the host, on some port `P`.
Firecracker forwards that connection to the host Unix socket `<uds_path>_P`.
Ports 2^30 to 2^31 - 1 are reserved for host-initiated connections. Pick a port below 2^30, such as 5252.
Source: [vsock.md](https://github.com/firecracker-microvm/firecracker/blob/main/docs/vsock.md), "Guest-Initiated Connections".

### Without the jailer

If the VM's `uds_path` is `/srv/vm1/v.sock`:

```sh
ln -s /run/atticdb/atticdb.sock /srv/vm1/v.sock_5252
```

One server serves every VM that has such a symlink.
The Firecracker process needs the server's uid or the socket group.

### With the jailer

The jailer builds a chroot at `<chroot_base>/<exec_file_name>/<id>/root`.
It enters a new mount namespace, pivots into the chroot, `chown`s the chroot root to `--uid` and `--gid`, then drops to them.
Source: [jailer.md](https://github.com/firecracker-microvm/firecracker/blob/main/docs/jailer.md).
It bind-mounts the chroot onto itself recursively, so a mount you made inside it first stays visible in the jail.
Source: [chroot.rs](https://github.com/firecracker-microvm/firecracker/blob/main/src/jailer/src/chroot.rs), `MS_BIND | MS_REC`.

Per VM, before you start its jailer:

```sh
root=/srv/jailer/firecracker/vm1/root
mkdir -p "$root/atticdb"
mount --bind /run/atticdb "$root/atticdb"
mount -o remount,bind,ro "$root/atticdb"
ln -s /atticdb/atticdb.sock "$root/v.sock_5252"   # the VM's uds_path is /v.sock
```

- Start the server with `--unix-socket-group <gid>`.
- Start each jailer with `--gid <gid>` and its own `--uid`.
- The symlink target is the path inside the jail.
- One server serves every VM through the same directory.

### In the guest

The Python client speaks vsock directly: `Client("vsock://2:5252", token)`.

`atticdb mcp` has no vsock support, because std Rust has no `AF_VSOCK`. Bridge it with `socat` 1.7.4 or later:

```sh
socat UNIX-LISTEN:/run/atticdb.sock,fork VSOCK-CONNECT:2:5252 &
ATTICDB_TOKEN=... atticdb mcp --url unix:///run/atticdb.sock
```

With `--url`, `atticdb mcp` never starts a server and skips the `LOCK` instance check.
`--token-file <path>` also works. The file must be 0600 and owned by the user.

## Containers (runc)

A container shares the host kernel. Mount the socket directory read-only and use `unix://`:

```sh
docker run -v /run/atticdb:/run/atticdb:ro --user <agent uid> --group-add <agents gid> <your agent image>
# inside: atticdb mcp --url unix:///run/atticdb/atticdb.sock
```

Run the process with its own uid and the socket group.

## gVisor

Mount the socket directory read-only as for runc. Also start `runsc` with `--host-uds=open`.
The default is `none`, which blocks `connect(2)` to host Unix sockets.
Source: [runsc/config/flags.go](https://github.com/google/gvisor/blob/master/runsc/config/flags.go) and [config.go](https://github.com/google/gvisor/blob/master/runsc/config/config.go), `HostUDS`.

## Kata Containers

Kata runs the container in a VM. Host directories reach it through filesystem sharing ([Limitations.md](https://github.com/kata-containers/kata-containers/blob/main/docs/Limitations.md), "hostPath volumes").
A Unix socket connects processes on the same machine ([unix(7)](https://man7.org/linux/man-pages/man7/unix.7.html)).
The guest has its own kernel, so a shared socket file does not reach the host server.
We have not tested a working path for Kata yet.

## Durability

AtticDB is only as durable as the storage under it. That is why it runs outside the sandbox.

- Firecracker's block `cache_type` defaults to `Unsafe`. It does not advertise flush to the guest.
  A host power loss can drop writes AtticDB inside the guest already acknowledged.
  If it must run inside, use `Writeback`, which `fsync`s the backing file on flush.
  Source: [block-caching.md](https://github.com/firecracker-microvm/firecracker/blob/main/docs/api_requests/block-caching.md).
- gVisor's default is `--overlay2=root:self`. Root filesystem writes go to a tmpfs overlay, not to the host files.
  Treat them as gone when the sandbox goes. `--overlay2=none` turns it off. Bind mounts are not overlaid by default.
  Source: [gVisor filesystem guide](https://gvisor.dev/docs/user_guide/filesystem/), "Filesystem Overlay".
