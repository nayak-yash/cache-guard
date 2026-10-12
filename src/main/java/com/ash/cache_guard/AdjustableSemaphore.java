package com.ash.cache_guard;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class AdjustableSemaphore {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition capacityAvailable = lock.newCondition();

    private int limit;
    private int active;

    public AdjustableSemaphore(int initialLimit) {
        if (initialLimit < 1) {
            throw new IllegalArgumentException("Limit must be positive");
        }
        this.limit = initialLimit;
    }

    public void acquire() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (active >= limit) {
                capacityAvailable.await();
            }
            active++;
        } finally {
            lock.unlock();
        }
    }

    public void release() {
        lock.lock();
        try {
            if (active <= 0) {
                throw new IllegalStateException("No active permit to release");
            }
            active--;
            capacityAvailable.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void resize(int newLimit) {
        if (newLimit < 1) {
            throw new IllegalArgumentException("Limit must be positive");
        }

        lock.lock();
        try {
            limit = newLimit;
            capacityAvailable.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public int getLimit() {
        lock.lock();
        try {
            return limit;
        } finally {
            lock.unlock();
        }
    }
}
