# Rusquitto Architecture: The Shared-Nothing Model

## 1. Philosophy: Shared-Nothing & Lock-Free Design

Rusquitto abandons the traditional multi-threaded model (Shared State) in favor of a distributed-system-on-a-chip
approach.

* **The Problem:** In standard architectures (e.g., Tokio with `Arc<Mutex<State>>`), as CPU core count increases, lock
  contention and cache coherency traffic (fighting over L1/L2 cache) degrade performance.
* **The Solution:** We treat every CPU core as an isolated "Shard". Each Shard owns its data exclusively. No locks are
  needed because no two threads ever access the same memory address simultaneously.

---

## 2. I/O Subsystem: Linux io_uring

We bypass standard POSIX syscalls (`read`, `write`, `epoll`) to communicate directly with the kernel via shared ring
buffers.

### Why io_uring?

* **Syscall Reduction:** `epoll` requires one syscall to check readiness and another to perform I/O. `io_uring` allows
  batched submission (submit 10 reads in 1 syscall).
* **True Asynchrony:** Unlike `epoll` (which is "readiness-based"), `io_uring` is "completion-based". The kernel
  performs the operation and notifies us only when the buffer is filled.
* **Zero-Copy Potential:** It enables direct data transfer between the Network Interface Card (NIC) and user-space
  buffers via DMA.

### How it is Implemented?

* We use the **Glommio** runtime.
* Each Shard initializes its own `io_uring` ring (Submission Queue & Completion Queue). This prevents synchronization
  overhead between cores at the kernel level.

---

## 3. Concurrency: Thread-per-Core Architecture

We enforce a strict 1:1 mapping between OS threads and physical CPU cores.

### Why Thread-per-Core?

* **Eliminating Context Switching:** Standard runtimes move tasks between threads ("Work Stealing"). This invalidates
  CPU caches. By pinning a thread to a core, data stays hot in the L1/L2 cache.
* **Deterministic Latency:** Without a global scheduler re-shuffling tasks, execution time becomes predictable.

### How it is Implemented?

* **CPU Affinity:** We use `sched_setaffinity` (via Glommio's `PoolPlacement`) to pin Executor 0 to CPU 0, Executor 1 to
  CPU 1, etc.
* **Local Task Scheduling:** Tasks (`spawn_local`) are executed strictly within the shard they were spawned in.

---

## 4. Network Ingress: Hardware-Level Load Balancing

Rusquitto eliminates the "Central Dispatcher" bottleneck by allowing all cores to accept connections simultaneously.

### Why SO_REUSEPORT?

* **The Bottleneck:** A single thread accepting connections and passing them to workers limits throughput to the speed
  of that one thread.
* **The Solution:** The Linux kernel's `SO_REUSEPORT` option allows multiple sockets to bind to the exact same IP and
  Port (`0.0.0.0:1883`).

### How it is Implemented?

* **Kernel Hashing:** When a TCP SYN packet arrives, the NIC/Kernel hashes the 4-tuple
  `(SrcIP, SrcPort, DstIP, DstPort)`.
* **Distribution:** Based on this hash, the kernel directs the packet to the incoming queue of a specific Shard's
  socket.
* **Result:** Load balancing happens in the OS/Hardware layer.

---

## 5. Inter-Shard Routing: Internal Message Bus

Since Shard 0 cannot read Shard 1's memory, they must communicate like separate servers.

### Why Message Passing?

* **Isolation:** To maintain the "Lock-Free" guarantee, we cannot share the subscription/client state.
* **Scalability:** This architecture allows Rusquitto to theoretically span across multiple physical machines with
  minimal changes.

### How it is Implemented?

* **Channel Mesh:** We build a Glommio `channel_mesh` (a full mesh of lock-free shared channels) connecting every
  shard to every other shard. Each shard `join()`s the mesh during `worker::init` and spawns a task that drains the
  inbound channels.
* **Broadcast routing:** A PUBLISH first fans out to local subscribers, then is forwarded to **every** peer shard,
  which runs its own local match against its subscription trie. This keeps each shard's state private — no shard ever
  queries another's tables.
* **Drop-on-full (current trade-off):** Forwarding uses a non-blocking `try_send_to`, so a slow peer never stalls the
  publisher. This means cross-shard QoS > 0 is currently best-effort; the at-least/exactly-once guarantee holds within
  a shard. Adding backpressure (an async `send_to`) is the path to full cross-shard QoS.

---

## 6. System Diagram (The Map)

This diagram illustrates the data flow from the physical network wire down to the specific CPU cores.

```text
      [ MQTT CLIENTS ]       [ MQTT CLIENTS ]
             |                      |
             |  (TCP / IP Traffic)  |
             v                      v
+-------------------------------------------------------+
|                LINUX KERNEL (Network Stack)           |
|                                                       |
|  [ NIC Hardware Queue ] -> [ 4-Tuple Hash Algo ]      |
|                                     |                 |
|            < SO_REUSEPORT Load Distribution >         |
+-------------------------------------------------------+
        |                 |                 |
        | (Socket 1)      | (Socket 2)      | (Socket 3)
        v                 v                 v
+---------------+ +---------------+ +---------------+
|  CPU CORE 0   | |  CPU CORE 1   | |  CPU CORE 2   |
| (Pinned T0)   | | (Pinned T1)   | | (Pinned T2)   |
|               | |               | |               |
| +-----------+ | | +-----------+ | | +-----------+ |
| | io_uring  | | | | io_uring  | | | | io_uring  | |
| +-----------+ | | +-----------+ | | +-----------+ |
| | LocalExec | | | | LocalExec | | | | LocalExec | |
| +-----------+ | | +-----------+ | | +-----------+ |
| | SessionMap| | | | SessionMap| | | | SessionMap| |
| +-----------+ | | +-----------+ | | +-----------+ |
+-------^-------+ +-------^-------+ +-------^-------+
        |                 |                 |
        |                 |                 |
+-------------------------------------------------------+
|           INTER-SHARD MESSAGE BUS (Mesh)              |
|   (Glommio channel_mesh — full mesh of shared chans)  |
+-------------------------------------------------------+
