const std = @import("std");
const net = std.net;
const posix = std.posix;
const Thread = std.Thread;
const Mutex = Thread.Mutex;
const Condition = Thread.Condition;

const SOCKET_FILE = "/tmp/rllm/daemon.sock";
const PROGRAM_PATH = "/mnt/pool/random-code/sleeper_agent/zig-out/bin/sleeper_agent";

const MAX_QUEUE = 2;

const Request = struct {
    pipe: [2]posix.fd_t,
    connection_fd: posix.fd_t,
    args: [][]u8,
    allocator: std.mem.Allocator,
};

const Queue = struct {
    buf: [MAX_QUEUE]Request = undefined,
    head: usize = 0,
    tail: usize = 0,
    count: usize = 0,
    mutex: Mutex = .{},
    condition: Condition = .{},

    fn push(self: *Queue, req: Request) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.count == MAX_QUEUE) return error.QueueFull;
        self.buf[self.tail] = req;
        self.tail = (self.tail + 1) % MAX_QUEUE;
        self.count += 1;
        self.condition.signal();
    }

    fn pop(self: *Queue) Request {
        self.mutex.lock();
        defer self.mutex.unlock();
        while (self.count == 0) self.condition.wait(&self.mutex);
        const req = self.buf[self.head];
        self.head = (self.head + 1) % MAX_QUEUE;
        self.count -= 1;
        return req;
    }
};

var global_queue: Queue = .{};

fn readArgs(conn_fd: posix.fd_t, allocator: std.mem.Allocator) ![][]u8 {
    var args = std.ArrayList([]u8).empty;
    errdefer {
        for (args.items) |item| allocator.free(item);
        args.deinit(allocator);
    }

    var line_buf: [4096]u8 = undefined;
    var pos: usize = 0;

    try args.append(allocator, try allocator.dupe(u8, PROGRAM_PATH));

    outer: while (true) {
        var byte: [1]u8 = undefined;
        const n = try posix.read(conn_fd, &byte);
        if (n == 0) break;

        if (byte[0] == '\n') {
            const line = line_buf[0..pos];
            pos = 0;

            if (std.mem.eql(u8, line, "run")) continue;
            if (std.mem.eql(u8, line, "END")) break :outer;

            try args.append(allocator, try allocator.dupe(u8, line));
        } else {
            if (pos < line_buf.len) {
                line_buf[pos] = byte[0];
                pos += 1;
            }
        }
    }

    return try args.toOwnedSlice(allocator);
}

fn workerThread(_: void) void {
    while (true) {
        const req = global_queue.pop();
        defer {
            for (req.args) |arg| req.allocator.free(arg);
            req.allocator.free(req.args);
            posix.close(req.pipe[0]);
            posix.close(req.pipe[1]);
            posix.close(req.connection_fd);
        }

        const result = runProgram(req.args, req.allocator, req.connection_fd);
        const reply: []const u8 = if (result) "ok\n" else |_| "err\n";
        _ = posix.write(req.connection_fd, reply) catch {};
    }
}

fn runProgram(args: [][]u8, allocator: std.mem.Allocator, conn_fd: posix.fd_t) !void {
    const const_args: []const []const u8 = @ptrCast(args);

    var child = std.process.Child.init(const_args, allocator);
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Pipe;
    try child.spawn();

    var buf: [4096]u8 = undefined;

    const child_stdout = child.stdout.?;
    while (true) {
        const n = child_stdout.read(&buf) catch break;
        if (n == 0) break;
        _ = posix.write(conn_fd, buf[0..n]) catch break;
    }

    const child_stderr = child.stderr.?;
    while (true) {
        const n = child_stderr.read(&buf) catch break;
        if (n == 0) break;
        _ = posix.write(conn_fd, buf[0..n]) catch break;
    }

    const term = try child.wait();
    switch (term) {
        .Exited => |code| if (code != 0) return error.ProgramFailed,
        else => return error.ProgramFailed,
    }
}

fn acceptLoop(server_fd: posix.fd_t) void {
    var da = std.heap.DebugAllocator(.{}).init;
    const allocator = da.allocator();

    while (true) {
        var addr: posix.sockaddr = undefined;
        var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr);
        const conn_fd = posix.accept(server_fd, &addr, &addr_len, 0) catch |err| {
            std.debug.print("[daemon] accept error: {}\n", .{err});
            continue;
        };

        const args = readArgs(conn_fd, allocator) catch |err| {
            _ = posix.write(conn_fd, "err\n") catch {};
            std.debug.print("[daemon] args error: {}\n", .{err});
            posix.close(conn_fd);
            continue;
        };

        var buf: [16]u8 = undefined;
        _ = posix.read(conn_fd, &buf) catch {};

        std.debug.print("[daemon] request received, queuing...\n", .{});

        const pipe_fds = posix.pipe() catch {
            _ = posix.write(conn_fd, "err\n") catch {};
            posix.close(conn_fd);
            continue;
        };

        const req = Request{
            .pipe = pipe_fds,
            .connection_fd = conn_fd,
            .allocator = allocator,
            .args = args,
        };

        global_queue.push(req) catch {
            std.debug.print("[daemon] queue full, rejecting\n", .{});
            _ = posix.write(conn_fd, "full\n") catch {};
            posix.close(pipe_fds[0]);
            posix.close(pipe_fds[1]);
            posix.close(conn_fd);
        };
    }
}

pub fn main() !void {
    std.fs.makeDirAbsolute("/tmp/rllm") catch |e| switch (e) {
        error.PathAlreadyExists => {},
        else => return e,
    };

    var location = try std.fs.openDirAbsolute("/tmp/rllm", .{ .iterate = true });
    try location.chmod(0o777);
    defer location.close();

    std.fs.deleteFileAbsolute(SOCKET_FILE) catch {};

    const server_fd = try posix.socket(posix.AF.UNIX, posix.SOCK.STREAM, 0);
    defer posix.close(server_fd);

    var sa = posix.sockaddr.un{ .family = posix.AF.UNIX, .path = undefined };
    @memset(&sa.path, 0);
    const path_bytes = SOCKET_FILE;
    @memcpy(sa.path[0..path_bytes.len], path_bytes);

    try posix.bind(server_fd, @ptrCast(&sa), @sizeOf(posix.sockaddr.un));
    try posix.listen(server_fd, MAX_QUEUE);

    std.debug.print("[daemon] listening on {s}\n", .{SOCKET_FILE});

    const worker = try Thread.spawn(.{}, workerThread, .{{}});
    worker.detach();

    acceptLoop(server_fd);
}
