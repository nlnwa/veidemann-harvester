package no.nb.nna.veidemann.commons.util;

import java.util.BitSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class Pool<T> implements AutoCloseable {
    private final int size;
    private final Lease<T>[] pool;
    private final BitSet leases;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Supplier<T> objectFactory;
    private final Predicate<T> verifier;
    private final Consumer<T> objectFinalizer;
    private final Consumer<Lease<T>> beforeLeaseFunc;
    private final Consumer<Lease<T>> afterReturnFunc;

    private final Lock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

    public Pool(int size, Supplier<T> objectFactory, Predicate<T> verifier, Consumer<T> objectFinalizer) {
        this(size, objectFactory, verifier, objectFinalizer, null, null);
    }

    public Pool(int size, Supplier<T> objectFactory, Predicate<T> verifier, Consumer<T> objectFinalizer,
                Consumer<Lease<T>> beforeLeaseFunc, Consumer<Lease<T>> afterReturnFunc) {
        this.size = size;
        this.objectFactory = objectFactory;
        this.verifier = verifier;
        this.objectFinalizer = objectFinalizer;
        this.beforeLeaseFunc = beforeLeaseFunc;
        this.afterReturnFunc = afterReturnFunc;
        leases = new BitSet(size);

        pool = new Lease[size];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = new Lease<>(this, i);
        }
    }

    public Lease<T> lease() throws InterruptedException {
        try {
            return lease(0, null);
        } catch (TimeoutException e) {
            // Should never happen
            throw new RuntimeException("Should never happen. This is a bug", e);
        }
    }

    public Lease<T> lease(long time, TimeUnit unit) throws InterruptedException, TimeoutException {
        if (closed.get()) {
            throw new IllegalStateException("Trying to lease object from closed pool");
        }

        Lease<T> result = findAvailableLease(time, unit);
        result.beforeLease(objectFactory, verifier, objectFinalizer);
        if (beforeLeaseFunc != null) {
            beforeLeaseFunc.accept(result);
        }
        return result;
    }

    private Lease<T> findAvailableLease(long time, TimeUnit unit) throws InterruptedException, TimeoutException {
        lock.lock();
        try {
            int nextAvailable = leases.nextClearBit(0);
            while (nextAvailable >= size) {
                if (unit == null) {
                    notEmpty.await();
                } else {
                    if (!notEmpty.await(time, unit)) {
                        throw new TimeoutException();
                    }
                }
                nextAvailable = leases.nextClearBit(0);
            }
            leases.set(nextAvailable);
            return pool[nextAvailable];
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws InterruptedException {
        if (closed.compareAndSet(false, true)) {
            // Wait for leases to be closed
            while (leases.cardinality() > 0) {
                lock.lock();
                try {
                    notEmpty.await();
                } finally {
                    lock.unlock();
                }
            }

            // Run finalizers
            for (int i = 0; i < size; i++) {
                pool[i].finalizeObject(objectFinalizer);
            }
        }
    }

    public static class Lease<T> implements AutoCloseable {
        private AtomicBoolean leased = new AtomicBoolean(false);
        private T object;
        private final int index;
        private final Pool pool;

        private Lease(Pool pool, int index) {
            this.pool = pool;
            this.index = index;
        }

        private void beforeLease(Supplier<T> objectFactory, Predicate<T> verifier, Consumer<T> objectFinalizer) {
            leased.compareAndSet(false, true);
            if (object == null) {
                object = objectFactory.get();
            } else if (verifier != null && !verifier.test(object)) {
                objectFinalizer.accept(object);
                object = objectFactory.get();
            }
        }

        private void finalizeObject(Consumer<T> objectFinalizer) {
            if (objectFinalizer != null) {
                objectFinalizer.accept(object);
            }
        }

        public T getObject() {
            if (leased.get()) {
                return object;
            } else {
                throw new IllegalStateException("Trying to use object from closed lease");
            }
        }

        @Override
        public void close() {
            if (pool.afterReturnFunc != null) {
                pool.afterReturnFunc.accept(this);
            }
            leased.compareAndSet(true, false);
            pool.lock.lock();
            try {
                pool.leases.clear(index);
                pool.notEmpty.signal();
            } finally {
                pool.lock.unlock();
            }
        }
    }
}
