const std = @import("std");
const net = std.net;
const posix = std.posix;
const Thread = std.Thread;
const Mutex = Thread.Mutex;
const Condition = Thread.Condition;

const MAX_QUEUE = 32;

const Request = struct {
    pipe: [2]posix.fd_t,
    conn_fd: posix.fd_t,
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

fn readArgs(conn_fd: posix.fd_t, allocator: std.mem.Allocator, program_path: []const u8) ![][]u8 {
    var args = std.ArrayList([]u8).empty;
    errdefer {
        for (args.items) |item| allocator.free(item);
        args.deinit(allocator);
    }

    var read_buf: [4096]u8 = undefined;
    var line_buf = std.ArrayList(u8).empty;
    defer line_buf.deinit(allocator);

    try args.append(allocator, try allocator.dupe(u8, program_path));

    outer: while (true) {
        const n = try posix.read(conn_fd, &read_buf);
        if (n == 0) break;

        for (read_buf[0..n]) |b| {
            if (b == '\n') {
                const line = line_buf.items;

                if (std.mem.eql(u8, line, "run")) {
                    line_buf.clearRetainingCapacity();
                    continue;
                }
                if (std.mem.eql(u8, line, "END")) break :outer;

                try args.append(allocator, try allocator.dupe(u8, line));
                line_buf.clearRetainingCapacity();
            } else {
                try line_buf.append(allocator, b);
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
            posix.close(req.conn_fd);
        }

        const result = runProgram(req.args, req.allocator, req.conn_fd);
        const reply: []const u8 = if (result) "ok\n" else |_| "err\n";
        _ = posix.write(req.conn_fd, reply) catch {};
    }
}

fn runProgram(args: [][]u8, allocator: std.mem.Allocator, conn_fd: posix.fd_t) !void {
    const const_args: []const []const u8 = @ptrCast(args);

    var child = std.process.Child.init(const_args, allocator);
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Inherit;
    try child.spawn();

    var poll_fds: [1]posix.pollfd = .{
        .{ .fd = child.stdout.?.handle, .events = posix.POLL.IN, .revents = 0 },
    };

    var line = std.ArrayList(u8).empty;
    defer line.deinit(allocator);

    var open_streams: usize = 1;
    while (open_streams > 0) {
        const ready_count = try posix.poll(&poll_fds, -1);
        if (ready_count == 0) continue;

        for (&poll_fds) |*pfd| {
            if ((pfd.revents & (posix.POLL.IN | posix.POLL.HUP)) != 0) {
                var read_buf: [4096]u8 = undefined;
                const n = try posix.read(pfd.fd, &read_buf);

                if (n == 0) {
                    open_streams -= 1;
                    continue;
                }
                var written: usize = 0;
                while (written < n) {
                    written += try posix.write(conn_fd, read_buf[written..n]);
                }

                pfd.revents = 0;
            }
        }
    }

    const term = try child.wait();
    switch (term) {
        .Exited => |code| if (code != 0) return error.ProgramFailed,
        else => return error.ProgramFailed,
    }
}

fn acceptLoop(server_fd: posix.fd_t, allocator: std.mem.Allocator, program_path: []const u8) void {
    while (true) {
        var addr: posix.sockaddr = undefined;
        var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr);
        const conn_fd = posix.accept(server_fd, &addr, &addr_len, 0) catch |err| {
            std.debug.print("[daemon] accept error: {}\n", .{err});
            continue;
        };

        const args = readArgs(conn_fd, allocator, program_path) catch |err| {
            _ = posix.write(conn_fd, "err\n") catch {};
            std.debug.print("[daemon] args error: {}\n", .{err});
            posix.close(conn_fd);
            continue;
        };

        std.debug.print("[daemon] request received, queuing...\n", .{});

        const pipe_fds = posix.pipe() catch {
            _ = posix.write(conn_fd, "err\n") catch {};
            posix.close(conn_fd);
            continue;
        };

        const req = Request{
            .pipe = pipe_fds,
            .conn_fd = conn_fd,
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
    var da = std.heap.DebugAllocator(.{}).init;
    defer {
        const leaked = da.deinit();
        std.debug.assert(leaked != std.heap.Check.ok);
    }
    const allocator = da.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    _ = args.next();

    const socket_dir = args.next() orelse {
        std.debug.print("an absolute location for the socket dir is required, pass it as an argument\n", .{});
        std.process.exit(1);
    };
    if (!std.fs.path.isAbsolute(socket_dir)) {
        std.debug.print("an absolute location for the socket dir is required, pass it as an argument\n", .{});
        std.process.exit(1);
    }
    const socket_file = try std.fs.path.join(
        allocator,
        &[_][]const u8{ socket_dir, "daemon.sock" },
    );
    defer allocator.free(socket_file);

    std.fs.makeDirAbsolute(socket_dir) catch |e| switch (e) {
        error.PathAlreadyExists => {},
        else => return e,
    };

    var location = try std.fs.openDirAbsolute(socket_dir, .{ .iterate = true });
    defer location.close();
    location.chmod(0o777) catch |err| {
        std.debug.print("error in chmod: {}\n", .{err});
        std.process.exit(1);
    };

    std.fs.deleteFileAbsolute(socket_file) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };

    const program_path = std.process.getEnvVarOwned(allocator, "PROGRAM_PATH") catch {
        std.debug.print("PROGRAM_PATH is not set!\n", .{});
        std.process.exit(1);
    };
    defer allocator.free(program_path);

    const server_fd = try posix.socket(posix.AF.UNIX, posix.SOCK.STREAM, 0);
    defer posix.close(server_fd);

    var sa = posix.sockaddr.un{ .family = posix.AF.UNIX, .path = undefined };
    @memset(&sa.path, 0);
    if (socket_file.len >= sa.path.len) return error.NameTooLong;
    @memcpy(sa.path[0..socket_file.len], socket_file);

    try posix.bind(server_fd, @ptrCast(&sa), @sizeOf(posix.sockaddr.un));
    try posix.listen(server_fd, MAX_QUEUE);

    std.debug.print("[daemon] listening on {s}\n", .{socket_file});

    const worker = try Thread.spawn(.{}, workerThread, .{{}});
    worker.detach();

    acceptLoop(server_fd, allocator, program_path);
}
