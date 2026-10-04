# Concurrency Interview Guide

A structured guide to the most commonly asked concurrency questions across FAANG
and top tech companies. Covers concepts, classic coding problems, and system design
scenarios. Java section first, Go section follows.

**How to use:** Work through each section in order. Each problem has a difficulty tag
and lists the core concepts it tests. Implement each one from scratch before looking
at library solutions.

---

# Part 1: Java Concurrency

## Section A: Core Concepts (Know Cold)

### 1. Thread Lifecycle & Creation
- `Thread` vs `Runnable` vs `Callable<V>`
- Thread states: NEW, RUNNABLE, BLOCKED, WAITING, TIMED_WAITING, TERMINATED
- `start()` vs `run()` — why calling `run()` directly doesn't create a new thread
- Daemon threads vs user threads

### 2. Java Memory Model (JMM)
- Main memory vs thread-local cache (CPU caches)
- Visibility problem: why one thread's write may not be seen by another
- `volatile` — guarantees visibility + prevents reordering, does NOT guarantee atomicity
- Happens-before relationships: `synchronized`, `volatile`, `Thread.start()`, `Thread.join()`
- Why `double-checked locking` breaks without `volatile`

### 3. synchronized, wait/notify
- Intrinsic locks (monitor locks) — one per object
- `synchronized` method vs `synchronized(obj)` block
- `wait()` releases the lock, `notify()` does not
- Spurious wakeups — always use `while` loop, never `if`
- `notifyAll()` vs `notify()` — when each is appropriate

### 4. Atomic Operations & CAS
- `AtomicInteger`, `AtomicLong`, `AtomicReference`
- Compare-And-Swap (CAS) — lock-free, CPU-level `CMPXCHG`
- ABA problem — `AtomicStampedReference`
- `LongAdder` / `LongAccumulator` — striped counters for write-heavy workloads
- When atomics beat locks, when they don't (high contention + complex invariants)

### 5. Locks (java.util.concurrent.locks)
- `ReentrantLock` vs `synchronized` — tryLock, timed lock, interruptible, fairness
- `ReadWriteLock` / `ReentrantReadWriteLock` — multiple readers OR one writer
- `StampedLock` — optimistic reads (Java 8+)
- Lock ordering to prevent deadlock
- `Condition` objects — multiple wait-sets per lock

### 6. Thread Safety Strategies
- Immutability (best strategy — no synchronization needed)
- Confinement: stack confinement, `ThreadLocal`
- Shared state with synchronization
- Copy-on-write (`CopyOnWriteArrayList`)

---

## Section B: Classic Coding Problems

### B0. Introduction — Producer-Consumer + Phased Worker Pool [Medium-Hard] ★★★★★
**Concepts:** Bounded buffer, blocking, barrier synchronization, phased execution

This is a two-part warmup that covers the two most fundamental concurrency patterns.

#### Part 1: Producer-Consumer (Bounded Queue)

Producer produces tasks, consumer consumes them. Producer waits when queue is full.
Both work in parallel.

**Java:**
```java
import java.util.LinkedList;
import java.util.Queue;

public class ProducerConsumer {
    private final Queue<Integer> queue = new LinkedList<>();
    private final int capacity;

    public ProducerConsumer(int capacity) {
        this.capacity = capacity;
    }

    public synchronized void produce(int item) throws InterruptedException {
        while (queue.size() == capacity) {
            wait();
        }
        queue.add(item);
        System.out.println(Thread.currentThread().getName()
            + " produced: " + item);
        notifyAll();
    }

    public synchronized int consume() throws InterruptedException {
        while (queue.isEmpty()) {
            wait();
        }
        int item = queue.poll();
        System.out.println(Thread.currentThread().getName()
            + " consumed: " + item);
        notifyAll();
        return item;
    }

    public static void main(String[] args) {
        ProducerConsumer pc = new ProducerConsumer(5);

        Thread producer = new Thread(() -> {
            for (int i = 0; i < 20; i++) {
                try {
                    pc.produce(i);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }, "Producer");

        Thread consumer = new Thread(() -> {
            for (int i = 0; i < 20; i++) {
                try {
                    pc.consume();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }, "Consumer");

        producer.start();
        consumer.start();
    }
}
```

**Go:**
```go
package main

import (
	"fmt"
	"sync"
)

func main() {
	queue := make(chan int, 5) // bounded buffer, capacity 5
	var wg sync.WaitGroup

	// Producer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			queue <- i // blocks if channel is full
			fmt.Printf("Produced: %d\n", i)
		}
		close(queue)
	}()

	// Consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for item := range queue { // blocks if channel is empty
			fmt.Printf("Consumed: %d\n", item)
		}
	}()

	wg.Wait()
}
```

#### Part 2: Phased Worker Pool

W workers, T tasks, P phases per task. All workers must complete phase N of ALL
tasks before any worker starts phase N+1. Workers work in parallel within a phase.

This is a **barrier synchronization** problem — use `CyclicBarrier` (Java) or
`sync.WaitGroup` as a barrier (Go).

**Java:**
```java
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;

public class PhasedWorkerPool {
    private final int numWorkers;
    private final int numTasks;
    private final int numPhases;

    public PhasedWorkerPool(int workers, int tasks, int phases) {
        this.numWorkers = workers;
        this.numTasks = tasks;
        this.numPhases = phases;
    }

    public void execute() throws InterruptedException {
        CyclicBarrier barrier = new CyclicBarrier(numWorkers);
        Thread[] workers = new Thread[numWorkers];

        for (int w = 0; w < numWorkers; w++) {
            final int workerId = w;
            workers[w] = new Thread(() -> {
                for (int phase = 0; phase < numPhases; phase++) {
                    // Each worker picks tasks round-robin
                    for (int t = workerId; t < numTasks;
                            t += numWorkers) {
                        System.out.printf(
                            "Worker %d | Task %d | Phase %d%n",
                            workerId, t, phase);
                    }
                    // Wait for ALL workers to finish this phase
                    try {
                        barrier.await();
                    } catch (InterruptedException
                            | BrokenBarrierException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }, "Worker-" + w);
            workers[w].start();
        }

        for (Thread t : workers) {
            t.join();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // 3 workers, 5 tasks, 3 phases
        new PhasedWorkerPool(3, 5, 3).execute();
    }
}
```

**Go:**
```go
package main

import (
	"fmt"
	"sync"
)

func main() {
	W := 3 // workers
	T := 5 // tasks
	P := 3 // phases

	var done sync.WaitGroup
	done.Add(W)

	// Shared barrier — reset for each phase
	var barrier sync.WaitGroup

	// Channel to coordinate phase start
	phaseCh := make(chan int, W)

	for w := 0; w < W; w++ {
		go func(workerID int) {
			defer done.Done()
			for phase := 0; phase < P; phase++ {
				// Each worker picks tasks round-robin
				for t := workerID; t < T; t += W {
					fmt.Printf(
						"Worker %d | Task %d | Phase %d\n",
						workerID, t, phase)
				}
				// Signal done with this phase, wait for others
				phaseCh <- phase
			}
		}(w)
	}

	// Coordinator: wait for all workers to finish each phase
	// before releasing them for the next
	go func() {
		for phase := 0; phase < P; phase++ {
			for w := 0; w < W; w++ {
				<-phaseCh
			}
			fmt.Printf("--- Phase %d complete ---\n", phase)
		}
	}()

	done.Wait()
}
```

**Simpler Go version using WaitGroup as barrier:**
```go
package main

import (
	"fmt"
	"sync"
)

func main() {
	W := 3
	T := 5
	P := 3

	for phase := 0; phase < P; phase++ {
		var wg sync.WaitGroup
		for w := 0; w < W; w++ {
			wg.Add(1)
			go func(workerID, p int) {
				defer wg.Done()
				for t := workerID; t < T; t += W {
					fmt.Printf(
						"Worker %d | Task %d | Phase %d\n",
						workerID, t, p)
				}
			}(w, phase)
		}
		wg.Wait() // barrier: all workers finish phase before next
		fmt.Printf("--- Phase %d complete ---\n", phase)
	}
}
```

**Key insight:** The barrier ensures no worker races ahead to the next phase.
`CyclicBarrier` (Java) is purpose-built for this. In Go, `sync.WaitGroup` per phase
or a coordinator goroutine achieves the same effect.

---

### B1. Producer-Consumer [Medium] ★★★★★
**Asked at:** Amazon, Google, Microsoft, Uber — most commonly asked concurrency problem
**Concepts:** Blocking queue, wait/notify, bounded buffer, back-pressure

Implement a bounded buffer (fixed-size queue) where:
- Producers block when buffer is full
- Consumers block when buffer is empty
- Thread-safe with proper signaling

```
Implement 3 ways:
1. synchronized + wait/notify
2. ReentrantLock + Condition (separate notFull, notEmpty)
3. Using BlockingQueue (know what's underneath)
```

**Variants:**
- Multiple producers, multiple consumers
- Priority producer-consumer (PriorityBlockingQueue)
- Single-producer single-consumer (lock-free ring buffer)

### B2. Print In Order / Sequence [Easy] ★★★★★
**Asked at:** Google, Amazon, LeetCode 1114
**Concepts:** Thread coordination, happens-before, signaling

Three threads call `first()`, `second()`, `third()`. Guarantee execution order.

```
Implement using:
1. CountDownLatch
2. Semaphore
3. volatile flags + busy-wait (explain why this is bad)
4. Lock + Condition
```

### B3. Print FooBar Alternately [Easy-Medium] ★★★★★
**Asked at:** Google, LeetCode 1115
**Concepts:** Turn-based coordination, mutual exclusion

Two threads: one prints "foo", other prints "bar". Must alternate: foobarfoobar...

```
Implement using:
1. Semaphore (two semaphores, ping-pong)
2. synchronized + wait/notify with boolean flag
3. Lock + Condition
```

### B3a. Print Odd-Even with 2 Threads [Easy] ★★★★★
**Asked at:** Amazon, Goldman Sachs, common screening round
**Concepts:** synchronized, wait/notify, turn-based coordination

Two threads: one prints odd numbers, other prints even numbers.
Output: 1 2 3 4 5 6 ... in order.

```
Implement using:
1. synchronized + wait/notify with boolean flag
2. Semaphore (two semaphores, ping-pong)
3. Lock + Condition
```

### B3b. Print Sequence Using N Threads [Medium] ★★★★
**Asked at:** Google, Amazon, Uber
**Concepts:** Lock + Condition, Semaphore, modular thread coordination

N threads. T1 prints 1, T2 prints 2, ... TN prints N, then T1 prints N+1, etc.
Output: 1 2 3 4 5 6 ... with each thread printing in round-robin.

```
Implement using:
1. Lock + Condition with shared counter (thread waits until counter % N == its id)
2. N Semaphores — each thread holds its own, releases the next thread's
```

### B4. Dining Philosophers [Medium] ★★★★
**Asked at:** Google, classic OS problem
**Concepts:** Deadlock, deadlock prevention, resource ordering, liveness

5 philosophers, 5 forks. Each needs two forks to eat.
Prevent deadlock AND starvation.

```
Solutions:
1. Resource ordering — always pick lower-numbered fork first
2. Arbitrator/waiter — ask permission before picking up forks
3. Chandy/Misra — message-passing solution
4. Limit concurrency — allow at most 4 to attempt simultaneously (Semaphore)
```

### B5. Reader-Writer Lock [Medium] ★★★★
**Asked at:** Amazon, Microsoft, Uber
**Concepts:** Shared vs exclusive access, starvation, fairness

Implement from scratch:
- Multiple readers can read simultaneously
- Writers need exclusive access
- Discuss reader-preference vs writer-preference vs fair

```
Implement:
1. Reader-preference (readers can starve writers)
2. Writer-preference (writers can starve readers)
3. Fair (FIFO ordering)
```

### B6. Bounded Blocking Queue [Medium] ★★★★★
**Asked at:** Amazon, Google, Microsoft, Uber — extremely common
**Concepts:** Synchronization, condition variables, circular buffer

Implement `BlockingQueue<T>` with:
- `put(T item)` — blocks if full
- `take()` — blocks if empty
- `size()`
- Fixed capacity

```
This is essentially the data structure behind Producer-Consumer.
Master this one thoroughly — it's the most frequently tested implementation.
```

### B7. Thread-Safe Singleton [Easy] ★★★★
**Asked at:** Almost everywhere
**Concepts:** Double-checked locking, volatile, class loading, initialization-on-demand

```
Know all 4 approaches:
1. Eager initialization (class loading guarantee)
2. Lazy with synchronized method (correct but slow)
3. Double-checked locking with volatile (most asked)
4. Enum singleton (Josh Bloch's recommendation)
5. Initialization-on-demand holder idiom (Bill Pugh)
```

### B8. Thread Pool Implementation [Hard] ★★★★
**Asked at:** Google, Amazon, Uber
**Concepts:** Worker threads, task queue, lifecycle management

Implement a basic `ThreadPool` with:
- Fixed number of worker threads
- Submit `Runnable`/`Callable` tasks
- Workers pull from a shared `BlockingQueue`
- Graceful shutdown

**Follow-up:** Implement `Future<T>` — how does `get()` block until result is ready?

### B9. Rate Limiter [Medium-Hard] ★★★★★
**Asked at:** Google, Uber, Stripe, Cloudflare
**Concepts:** Token bucket, sliding window, thread safety, time-based logic

Implement a thread-safe rate limiter:
- `boolean allow()` — returns true if request is allowed
- N requests per second

```
Algorithms to know:
1. Token bucket (most common)
2. Sliding window counter
3. Leaky bucket
Know trade-offs between them.
```

### B10. H2O Problem [Medium] ★★★★
**Asked at:** Google, LeetCode 1117
**Concepts:** Barrier synchronization, semaphores, grouping

Multiple threads call `hydrogen()` and `oxygen()`.
Group them into water molecules: every 2 H threads + 1 O thread release together.

```
Implement using:
1. Semaphore (H semaphore init=2, O semaphore init=1) + CyclicBarrier
2. ReentrantLock + Condition
```

### B11. Concurrent HashMap / Thread-Safe Cache [Hard] ★★★★
**Asked at:** Amazon, Google, Goldman Sachs
**Concepts:** Segmented locking, CAS, read-write locks, cache eviction

Implement a thread-safe key-value store with:
- `get(key)`, `put(key, value)`
- Fine-grained locking (not one big lock)

**Variant:** LRU Cache with thread safety
- Combine doubly-linked list + HashMap
- Synchronization strategy: lock the whole structure vs lock striping

### B12. Scheduled Task Executor [Hard] ★★★
**Asked at:** Google, Amazon
**Concepts:** Priority queue, delayed execution, timer threads

Implement a scheduler that:
- Accepts tasks with a delay or at fixed rate
- Uses a min-heap ordered by execution time
- Worker thread(s) poll and execute

### B13. CountDownLatch from Scratch [Medium] ★★★
**Asked at:** Amazon, Microsoft
**Concepts:** AQS (AbstractQueuedSynchronizer), shared mode, state management

Implement:
- `CountDownLatch(int count)`
- `countDown()` — decrements, releases all waiters when reaches 0
- `await()` — blocks until count reaches 0

### B14. CyclicBarrier from Scratch [Medium] ★★★
**Asked at:** Google
**Concepts:** Barrier synchronization, reusability, generation

Implement:
- `CyclicBarrier(int parties)`
- `await()` — blocks until all parties arrive, then releases all
- Reusable (unlike CountDownLatch)

### B15. Deadlock Detection [Medium] ★★★
**Asked at:** Amazon, Microsoft
**Concepts:** Resource allocation graph, cycle detection, wait-for graph

Given a system of threads and locks:
- Build a wait-for graph
- Detect cycles (DFS)
- Discuss resolution strategies

### B16. FizzBuzz Multithreaded [Easy-Medium] ★★★
**Asked at:** LeetCode 1195
**Concepts:** Thread coordination, modular synchronization

Four threads: fizz, buzz, fizzbuzz, number.
Each handles its respective case for numbers 1 to n.

### B17. Web Crawler Multithreaded [Medium-Hard] ★★★★
**Asked at:** Google, LeetCode 1242
**Concepts:** Thread pool, visited set (ConcurrentHashMap), BFS, domain filtering

Crawl URLs using multiple threads:
- Don't visit same URL twice
- Stay within same hostname
- Bounded parallelism

### B18. Traffic Light / Intersection [Medium] ★★★
**Asked at:** Google, LeetCode 1279
**Concepts:** Mutual exclusion, fairness, state machine

One intersection, two roads. Only one road has green at a time.
Cars arrive on both roads. Minimize light switches.

### B19. Unisex Bathroom Problem [Medium] ★★★
**Asked at:** Classic OS / systems interviews
**Concepts:** Reader-writer variant, capacity constraints, fairness

A bathroom that allows:
- Multiple men OR multiple women (not both)
- Maximum N people at once
- No starvation

### B20. Build a Future/Promise [Medium-Hard] ★★★★
**Asked at:** Google, Amazon
**Concepts:** Blocking, signaling, exception propagation, composition

Implement `Future<T>`:
- `get()` — blocks until result available
- `get(timeout)` — blocks with timeout
- `isDone()`, `cancel()`
- Stores either result or exception

### B21. Merge Results from Parallel API Calls [Medium] ★★★★
**Asked at:** Amazon, Uber, Stripe
**Concepts:** CompletableFuture, CountDownLatch, parallel execution, result aggregation

Call 3 independent APIs in parallel, wait for all to complete, merge results.
Handle: partial failure (one API fails), timeouts, cancellation.

```
Implement using:
1. CompletableFuture.allOf() + thenCombine/thenApply
2. ExecutorService + Future.get(timeout)
3. CountDownLatch — each API call counts down, main thread awaits
```

### B22. Async Task Scheduler with Dependencies [Hard] ★★★★
**Asked at:** Google, Amazon, build-system style problems
**Concepts:** Topological sort, CountDownLatch/CompletableFuture, DAG execution

Tasks have dependencies (like a build system: compile before link).
Execute tasks with maximum parallelism while respecting dependency order.

```
Approach:
1. Build DAG, topological sort
2. Each task has a CountDownLatch initialized to its in-degree
3. When a task completes, decrement latch of all dependents
4. Tasks with latch=0 are submitted to thread pool

Alternative: CompletableFuture chaining — each task's future depends on
thenCombine of its prerequisite futures.
```

---

## Section C: System Design Concurrency Scenarios

These appear in system design rounds but require deep concurrency knowledge:

| Scenario | Key Concurrency Concepts |
|---|---|
| Design a connection pool | Bounded buffer, semaphore, health checking, idle timeout |
| Design a distributed rate limiter | Token bucket + Redis atomic ops, sliding window |
| Global request counter (10 instances) | Sharded counters, LongAdder, eventual consistency |
| Design a job scheduler | Priority queue, worker pool, at-least-once delivery |
| Design a concurrent LRU cache | Lock striping, read-write locks, eviction under lock |
| Design a pub-sub system | Producer-consumer, fan-out, backpressure |
| Design a thread-safe logger | Ring buffer, async appender, batch flushing |

---

# Part 2: Go Concurrency

## Section A: Core Concepts (Know Cold)

### 1. Goroutines
- Goroutine vs OS thread — M:N scheduling, ~2KB initial stack (grows dynamically)
- `go func()` — fire and forget, no return value
- Goroutine leak — a goroutine blocked forever (channel/mutex) is a memory leak
- `runtime.GOMAXPROCS` — number of OS threads for parallel execution
- `runtime.Gosched()` — yield, rarely needed

### 2. Channels
- Unbuffered — synchronous handoff, sender blocks until receiver is ready and vice versa
- Buffered — async up to capacity, blocks when full/empty
- Directional channels: `chan<- T` (send-only), `<-chan T` (receive-only)
- `close()` — signals no more values; receiving from closed channel returns zero value
- `range` over channel — iterates until channel is closed
- `select` — multiplex across multiple channel operations, `default` for non-blocking

### 3. sync Package
- `sync.Mutex` / `sync.RWMutex` — same semantics as Java ReentrantLock / ReadWriteLock
- `sync.WaitGroup` — like Java's CountDownLatch
- `sync.Once` — exactly-once initialization (singleton)
- `sync.Map` — concurrent map (limited use cases; often a regular map + RWMutex is better)
- `sync.Pool` — object pool for reducing GC pressure
- `sync.Cond` — condition variable (rarely used; channels usually preferred)

### 4. Context
- `context.Context` — cancellation, deadline, timeout propagation
- `context.WithCancel`, `WithTimeout`, `WithDeadline`, `WithValue`
- Every long-running goroutine should accept and respect a context
- Cancellation cascades through the tree

### 5. Patterns
- **Fan-out / Fan-in** — multiple goroutines read from one channel; multiple goroutines write to one channel
- **Pipeline** — chain of stages connected by channels
- **Worker pool** — N goroutines pulling from a shared job channel
- **Semaphore via buffered channel** — `make(chan struct{}, N)` limits concurrency
- **errgroup** — `golang.org/x/sync/errgroup` — wait for goroutines with error propagation
- **Ticker / Timer** — periodic or delayed execution

### 6. Common Pitfalls
- Loop variable capture (pre Go 1.22): `for _, v := range items { go func() { use(v) }() }` — all goroutines see last value
- Goroutine leak: forgetting to close a channel, or not draining
- Race conditions: `go test -race` is your best friend
- Closing a channel twice panics
- Sending on a closed channel panics
- Zero-value reads from closed channel — use `val, ok := <-ch`

---

## Section B: Classic Coding Problems (Go)

### B1. Producer-Consumer with Channels [Easy-Medium] ★★★★★
**Concepts:** Buffered channels as bounded buffer, goroutines, `sync.WaitGroup`

```
Implement:
1. Buffered channel as the queue
2. N producer goroutines, M consumer goroutines
3. Graceful shutdown with close(channel) + WaitGroup
```

Go's channels make this much simpler than Java — the channel IS the blocking queue.

### B2. Worker Pool [Medium] ★★★★★
**Concepts:** Goroutines, channels, fan-out

```
Implement:
- Fixed N worker goroutines
- Jobs submitted via channel
- Results collected via channel
- Graceful shutdown
```

### B3. Rate Limiter [Medium] ★★★★★
**Concepts:** `time.Ticker`, token bucket, `select`

```
Implement:
1. time.Tick based (simple, bursty)
2. Token bucket with goroutine refilling tokens into buffered channel
3. golang.org/x/time/rate (know the library)
```

### B4. Fan-Out / Fan-In Pipeline [Medium] ★★★★
**Concepts:** Pipeline pattern, channel composition, done channel for cancellation

```
Implement a 3-stage pipeline:
1. Generator: produces numbers
2. Square: squares each number (fan-out to N workers)
3. Merge: fan-in results from all workers into one channel
With context cancellation to tear down cleanly.
```

### B5. Concurrent Web Crawler [Medium-Hard] ★★★★
**Concepts:** Goroutines, sync.Map or mutex+map, semaphore, BFS

```
Implement:
- Crawl starting from a seed URL
- Bounded concurrency (max N in-flight requests)
- Deduplication (don't visit same URL twice)
- Use context for timeout
```

### B6. Dining Philosophers [Medium] ★★★★
**Concepts:** Channels as forks, goroutines as philosophers

```
Solutions in Go:
1. Channel-per-fork — philosopher must receive from both fork channels
2. Waiter goroutine — arbitrator pattern
3. Resource ordering with sync.Mutex
```

### B7. Read-Write Lock from Channels [Medium-Hard] ★★★
**Concepts:** Channel-based synchronization, select

```
Implement RWMutex semantics using only channels:
- readReq, readRelease, writeReq, writeRelease channels
- A manager goroutine that tracks state
```

Shows deep understanding of Go's CSP model.

### B8. Concurrent-Safe Cache with TTL [Medium-Hard] ★★★★
**Concepts:** sync.RWMutex, time.AfterFunc, lazy vs active eviction

```
Implement:
- Get(key), Set(key, value, ttl)
- Thread-safe
- Expired entries cleaned up
```

### B9. Timeout and Cancellation Patterns [Medium] ★★★★
**Concepts:** context, select, done channels

```
Implement:
1. First-response-wins: fan-out to 3 replicas, take first response, cancel others
2. Timeout: make an RPC call with a deadline, return error if too slow
3. Heartbeat: goroutine sends periodic heartbeats, caller detects if it stops
```

### B10. Bounded Parallel Execution [Easy-Medium] ★★★★
**Concepts:** Semaphore pattern, errgroup

```
Given 1000 URLs, fetch all with max 10 concurrent requests.
Implement:
1. Buffered channel as semaphore
2. errgroup with SetLimit
```

### B11. Pubsub / Event Bus [Medium] ★★★★
**Concepts:** Multiple subscribers, fan-out, goroutine per subscriber

```
Implement:
- Subscribe(topic) returns <-chan Event
- Publish(topic, event) sends to all subscribers
- Unsubscribe
- Non-blocking publish (drop if subscriber is slow, or buffer)
```

### B12. Pipeline with Error Handling [Medium] ★★★★
**Concepts:** errgroup, context cancellation, partial failure

```
Implement a pipeline where:
- Any stage can fail
- Failure cancels all stages
- First error is returned
- All goroutines clean up (no leaks)
```

### B13. Barrier / Phaser [Medium] ★★★
**Concepts:** sync.WaitGroup or channels as barrier

```
Implement CyclicBarrier in Go:
- N goroutines call Wait()
- All block until N arrive
- Then all release
- Reusable for next round
```

### B14. Merge N Sorted Channels [Medium-Hard] ★★★
**Concepts:** Heap + channels, fan-in

```
Given N sorted channels, merge into one sorted output channel.
Use a min-heap of (value, channel-index).
```

### B15. Graceful Shutdown [Medium] ★★★★★
**Concepts:** os.Signal, context, WaitGroup, draining

```
Implement a server that:
- Catches SIGINT/SIGTERM
- Stops accepting new work
- Waits for in-flight work to complete (with timeout)
- Exits cleanly
```

This is asked in almost every Go systems interview.

---

## Section C: Go-Specific Theory Questions

These come up as verbal/discussion questions:

| Question | What They Want to Hear |
|---|---|
| Channels vs Mutex — when to use which? | Channels for ownership transfer/signaling, mutex for protecting shared state. "Share memory by communicating" |
| How does the Go scheduler work? | M:N scheduling, G (goroutine), M (OS thread), P (processor). Work-stealing. Preemption since Go 1.14 |
| What is a goroutine leak? How to prevent? | Blocked goroutine never exits. Use context, done channels, or ensure channels are drained/closed |
| How does `select` work with multiple ready cases? | Pseudo-random choice among ready cases |
| Unbuffered vs buffered channel? | Unbuffered = rendezvous/synchronization. Buffered = async with capacity. Buffered hides bugs |
| What does `go test -race` do? | Instruments memory accesses at compile time, detects data races at runtime (ThreadSanitizer) |
| Context best practices? | Always first param, never store in struct, cancel ASAP, don't pass nil |
| sync.Pool — when useful? | Short-lived objects with high allocation rate (JSON encoders, byte buffers). Cleared on GC |

---

## Study Order (Recommended)

### Week 1: Foundations + Easy Problems
1. Review JMM, volatile, synchronized (Java) or goroutines, channels (Go)
2. Print In Order (B2), FooBar (B3), FizzBuzz (B16)
3. Thread-Safe Singleton (B7) / sync.Once
4. Producer-Consumer (B1)

### Week 2: Core Problems
5. Bounded Blocking Queue (B6)
6. Reader-Writer Lock (B5)
7. Rate Limiter (B9)
8. Worker Pool / Thread Pool (B8)

### Week 3: Hard Problems + Design
9. Dining Philosophers (B4)
10. H2O Problem (B10)
11. Web Crawler Multithreaded (B17)
12. Build a Future/Promise (B20)
13. System design concurrency scenarios

### Week 4: Deep Dive + Practice
14. CountDownLatch/CyclicBarrier from scratch
15. Concurrent LRU Cache
16. Scheduled Task Executor
17. Mock interviews — pick random problems, implement under time pressure

---

## Quick Reference: Java Concurrency Cheat Sheet

| Need | Use |
|---|---|
| Mutual exclusion | `synchronized` or `ReentrantLock` |
| Read-heavy shared data | `ReentrantReadWriteLock` or `StampedLock` |
| Atomic counter (low contention) | `AtomicLong` |
| Atomic counter (high contention) | `LongAdder` |
| Thread-safe queue | `ConcurrentLinkedQueue` (unbounded), `ArrayBlockingQueue` (bounded) |
| Thread-safe map | `ConcurrentHashMap` |
| Wait for N tasks | `CountDownLatch` |
| Barrier for N threads | `CyclicBarrier` |
| Limit concurrency | `Semaphore` |
| One-time init | `static final` or double-checked locking |
| Thread-local data | `ThreadLocal<T>` |
| Async computation | `CompletableFuture` |
| Thread pool | `Executors.newFixedThreadPool(N)` |

## Quick Reference: Go Concurrency Cheat Sheet

| Need | Use |
|---|---|
| Mutual exclusion | `sync.Mutex` |
| Read-heavy shared data | `sync.RWMutex` |
| Atomic counter | `atomic.AddInt64` or `atomic.Int64` (Go 1.19+) |
| Thread-safe map | `sync.Map` or `map` + `sync.RWMutex` |
| Wait for N goroutines | `sync.WaitGroup` |
| Limit concurrency | Buffered channel `make(chan struct{}, N)` or `errgroup.SetLimit` |
| One-time init | `sync.Once` |
| Cancellation/timeout | `context.Context` |
| Bounded queue | Buffered channel |
| Worker pool | N goroutines reading from job channel |
| Periodic task | `time.Ticker` |
| Select first result | `select` with multiple channels |
