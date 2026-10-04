package com.example.demo;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// Every object has a hidden JVM-level monitor --> synchrozied acquires it --> wait/notify operate on it
// java intrinsic monitor (synchronized)  is reentrant, then why ReentrantLock, it exist because it gives more control
// 1. tryLock() 2. Fairness option 3. Multiple conditions 4. More explicit control over locking/unlocking happens.

// monitor is key (jvm level mechanism associated with obj) not variable
// only when you have monitor, you can call wait, notify
// take synchronized(lock) then, we can call wait notify, the thing is you have to go default way.

/**
 * Question :
 *
 * The setup: You have a shared buffer (queue) with a fixed capacity — say 10 slots. Multiple producer threads generate items and put them in the buffer. Multiple consumer threads take items out and process them.
 *
 * The constraints:
 *
 * Buffer full — A producer calls put(item) but all 10 slots are occupied. It must block (wait/sleep) until a consumer removes something, freeing a slot. It should NOT spin-loop, busy-wait, or throw an error.
 * Buffer empty — A consumer calls take() but the buffer has 0 items. It must block until a producer adds something. Same rules — no spinning.
 * Thread safety — If 5 producers and 5 consumers all call put/take simultaneously, no item is lost, duplicated, or corrupted. No race conditions.
 * Proper signaling — When a producer adds an item to a previously-empty buffer, it must wake up a blocked consumer (and vice versa). Without signaling, blocked threads sleep forever.
 * What you're building:
 *
 *
 *
 * class BoundedBuffer<T> {
 *     BoundedBuffer(int capacity)   // fixed-size buffer
 *     void put(T item)              // blocks if full, adds item, signals consumers
 *     T take()                      // blocks if empty, removes item, signals producers
 * }
 * The tricky parts:
 *
 * Lost wakeups — Thread A signals before Thread B starts waiting. B never wakes up.
 * Spurious wakeups — OS can wake a thread for no reason. Your wait must be in a while loop, not an if.
 * Two conditions, one lock — "not full" and "not empty" are separate conditions. Using notify() can wake the wrong thread (a producer waking another producer). You need either notifyAll() or separate Condition objects.
 * Implementation choices (pick one):
 *
 * synchronized + wait()/notifyAll() — simplest
 * ReentrantLock + two Condition objects (notFull, notEmpty) — more precise signaling, preferred in interviews
 * Semaphore pair — one counting empty slots, one counting full slots
 *
 */

public class BoundedProducerConsumer {

    Queue<Integer> buffer;
    Integer capacity;
    Lock lock;
    Object obj;

    public BoundedProducerConsumer(Integer capacity) {
        this.buffer = new LinkedList<>();
        this.capacity = capacity;
        this.lock = new ReentrantLock();
        this.obj = new Object();
    }

    void put(int value) throws InterruptedException {

       synchronized (obj){
            while(capacity.equals(buffer.size())) {
                System.out.println("Thread waiting inside put");
                obj.wait();
            }
            buffer.add(value);
            obj.notifyAll();

        }
        //if capacity is less
    }

    int get() throws InterruptedException{
        synchronized (obj)
        {
            while(buffer.size() == 0)
            {
                obj.wait();
            }
            Thread.sleep(200);
            int val = buffer.poll();
            obj.notifyAll();
            return val;
        }
    }

    public static void main(String[] args) {
        BoundedProducerConsumer boundedProducerConsumer = new BoundedProducerConsumer(5);
        Thread t1 = new Thread(()->{
            for(int i=0;i<100;i++){
                try {
                    boundedProducerConsumer.put(i);
                    System.out.println("value "+ i + " inserted.");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        Thread t2 = new Thread(()->{
            for(int i=0;i<100;i++){
                try{
                    System.out.println(boundedProducerConsumer.get());
                }catch (InterruptedException e){
                    throw new RuntimeException(e);
                }
            }
        });

        t1.start();
        t2.start();
    }

}


/***
 * # Java Synchronization: Monitor vs ReentrantLock
 *
 * ## 1. Every Java Object Has a Monitor
 *
 * Conceptually, every Java object has a **monitor** associated with it.
 *
 * ```text
 * Object
 *   │
 *   └── Monitor 🔐
 * ```
 *
 * The monitor is a **JVM-level mechanism**, not a normal Java variable that we can access directly.
 *
 * ```java
 * obj.monitor; // ❌ doesn't exist
 * ```
 *
 * ---
 *
 * ## 2. `synchronized` Acquires the Object's Monitor
 *
 * ```java
 * synchronized (obj) {
 *     // critical section
 * }
 * ```
 *
 * This means:
 *
 * > The current thread acquires `obj`'s monitor.
 *
 * Only one thread can own that monitor at a time.
 *
 * ```text
 * Thread A
 *    │
 *    ▼
 * obj's Monitor 🔐
 *
 * Thread B → waits
 * Thread C → waits
 * ```
 *
 * When Thread A leaves the synchronized block, the monitor is released.
 *
 * ---
 *
 * ## 3. `wait()` / `notify()` / `notifyAll()`
 *
 * These methods belong to `Object`:
 *
 * ```java
 * obj.wait();
 * obj.notify();
 * obj.notifyAll();
 * ```
 *
 * They operate on the **object's monitor**.
 *
 * Therefore, the current thread **must own that object's monitor** before calling them.
 *
 * Correct:
 *
 * ```java
 * synchronized (obj) {
 *     obj.wait();
 * }
 * ```
 *
 * Correct:
 *
 * ```java
 * synchronized (obj) {
 *     obj.notifyAll();
 * }
 * ```
 *
 * Incorrect:
 *
 * ```java
 * obj.wait();       // ❌
 * obj.notifyAll();  // ❌
 * ```
 *
 * This causes:
 *
 * ```text
 * IllegalMonitorStateException
 * ```
 *
 * because the current thread does not own `obj`'s monitor.
 *
 * ---
 *
 * ## 4. What Does `wait()` Do?
 *
 * When a thread executes:
 *
 * ```java
 * synchronized (obj) {
 *     obj.wait();
 * }
 * ```
 *
 * it:
 *
 * 1. Releases `obj`'s monitor.
 * 2. Goes into the waiting state.
 * 3. Waits for another thread to call `obj.notify()` or `obj.notifyAll()`.
 * 4. After being notified, it tries to reacquire `obj`'s monitor.
 * 5. Only after reacquiring the monitor does `wait()` return.
 *
 * This is why `wait()` is useful for producer-consumer problems.
 *
 * ---
 *
 * ## 5. `notifyAll()`
 *
 * Suppose multiple threads are waiting on an object's monitor:
 *
 * ```text
 *              obj
 *               │
 *         ┌─────┼─────┐
 *         ↓     ↓     ↓
 *        T1    T2    T3
 *      waiting waiting waiting
 * ```
 *
 * Calling:
 *
 * ```java
 * obj.notifyAll();
 * ```
 *
 * wakes all those waiting threads.
 *
 * They then compete to reacquire `obj`'s monitor.
 *
 * ---
 *
 * # ReentrantLock
 *
 * ## 6. `ReentrantLock` Is Also an Object — But Different!
 *
 * ```java
 * ReentrantLock lock = new ReentrantLock();
 * ```
 *
 * Yes, `lock` is a Java object.
 *
 * However:
 *
 * ```java
 * lock.lock();
 * ```
 *
 * does **not** acquire the Java object's intrinsic monitor.
 *
 * It acquires the **ReentrantLock's own locking mechanism**.
 *
 * There are therefore two different concepts:
 *
 * ```text
 * synchronized(obj)
 *        │
 *        ▼
 * Object's Monitor 🔐
 *
 *
 * lock.lock()
 *        │
 *        ▼
 * ReentrantLock's Lock 🔐
 * ```
 *
 * They are separate mechanisms.
 *
 * ---
 *
 * ## 7. Why `lock.notifyAll()` Doesn't Work
 *
 * This:
 *
 * ```java
 * lock.lock();
 *
 * try {
 *     lock.notifyAll();   // ❌
 * } finally {
 *     lock.unlock();
 * }
 * ```
 *
 * throws:
 *
 * ```text
 * IllegalMonitorStateException
 * ```
 *
 * because `lock.lock()` gives you ownership of the **ReentrantLock**, not ownership of the **object's intrinsic monitor**.
 *
 * `notifyAll()` specifically requires ownership of the object's monitor.
 *
 * You technically could do:
 *
 * ```java
 * synchronized (lock) {
 *     lock.notifyAll();   // ✅
 * }
 * ```
 *
 * but this is mixing two different synchronization mechanisms and is generally not what you want.
 *
 * ---
 *
 * # ReentrantLock + Condition
 *
 * When using `ReentrantLock`, use `Condition` instead of `wait/notify`.
 *
 * ```java
 * ReentrantLock lock = new ReentrantLock();
 * Condition condition = lock.newCondition();
 * ```
 *
 * The equivalents are:
 *
 * | `synchronized`      | `ReentrantLock`         |
 * | ------------------- | ----------------------- |
 * | `synchronized(obj)` | `lock.lock()`           |
 * | `obj.wait()`        | `condition.await()`     |
 * | `obj.notify()`      | `condition.signal()`    |
 * | `obj.notifyAll()`   | `condition.signalAll()` |
 *
 * Example:
 *
 * ```java
 * lock.lock();
 *
 * try {
 *     while (queue.isEmpty()) {
 *         condition.await();
 *     }
 *
 *     // consume
 *     condition.signalAll();
 *
 * } finally {
 *     lock.unlock();
 * }
 * ```
 *
 * ---
 *
 * # Why Use `ReentrantLock`?
 *
 * `ReentrantLock` is **not needed because `synchronized` isn't reentrant**.
 *
 * In fact, **both are reentrant**.
 *
 * `ReentrantLock` provides additional control:
 *
 * * `tryLock()` — attempt to acquire without waiting indefinitely
 * * `lockInterruptibly()` — allow lock acquisition to be interrupted
 * * Fairness option
 * * Multiple `Condition`s
 * * Explicit lock/unlock control
 *
 * For example:
 *
 * ```java
 * if (lock.tryLock()) {
 *     try {
 *         // critical section
 *     } finally {
 *         lock.unlock();
 *     }
 * }
 * ```
 *
 * ---
 *
 * # Reentrancy
 *
 * Both `synchronized` and `ReentrantLock` are **reentrant**.
 *
 * The same thread can acquire the same lock multiple times.
 *
 * ```java
 * synchronized (obj) {
 *     synchronized (obj) {
 *         // ✅ allowed
 *     }
 * }
 * ```
 *
 * The JVM keeps track of how many times the thread acquired the monitor.
 *
 * The monitor is actually released only after the corresponding number of exits.
 *
 * ---
 *
 * # Mental Model
 *
 * Remember this:
 *
 * ```text
 *                 JAVA LOCKING
 *                      │
 *           ┌──────────┴──────────┐
 *           │                     │
 *      synchronized          ReentrantLock
 *           │                     │
 *           ▼                     ▼
 *    Object Monitor 🔐      ReentrantLock 🔐
 *           │                     │
 *           ▼                     ▼
 *    wait/notify             Condition
 * ```
 *
 * ### One-line summary
 *
 * > **Every Java object has a JVM-level monitor. `synchronized` acquires that monitor,
 * and `wait/notify/notifyAll` require ownership of it.
 * `ReentrantLock` is a separate locking mechanism, so it uses `Condition.await/signal/signalAll` instead.**
 */

