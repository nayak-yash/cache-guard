package com.ash.cache_guard;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Repository
public class ItemRepository {

    private final JdbcTemplate jdbcTemplate;

    private final AdjustableSemaphore dbSemaphore;

    private final AdaptiveConcurrencyController controller;

    private final boolean controllerEnabled;

    private final AtomicInteger activeQueries = new AtomicInteger();
    private final AtomicInteger peakConcurrentQueries = new AtomicInteger();

    private final AtomicLong totalQueries = new AtomicLong();

    private final AtomicLong totalQueryTimeNanos = new AtomicLong();
    private final AtomicLong maxQueryTimeNanos = new AtomicLong();

    private final AtomicLong permitWaitCount = new AtomicLong();
    private final AtomicLong totalPermitWaitNanos = new AtomicLong();
    private final AtomicLong maxPermitWaitNanos = new AtomicLong();

    public ItemRepository(JdbcTemplate jdbcTemplate,
                          @Value("${cache-guard.db.max-concurrency:5}") int semaphoreLimit,
                          @Value("${cache-guard.controller.enabled:true}") boolean controllerEnabled) {
        this.jdbcTemplate = jdbcTemplate;
        this.dbSemaphore = new AdjustableSemaphore(semaphoreLimit);
        this.controller = new AdaptiveConcurrencyController(dbSemaphore);
        this.controllerEnabled = controllerEnabled;
    }

    public Optional<Item> findByKey(String key) {
        long waitStart = System.nanoTime();

        try {
            dbSemaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while waiting for a database permit", e);
        }

        long waitNanos = System.nanoTime() - waitStart;
        permitWaitCount.incrementAndGet();
        totalPermitWaitNanos.addAndGet(waitNanos);
        maxPermitWaitNanos.accumulateAndGet(waitNanos, Math::max);

        totalQueries.incrementAndGet();

        int active = activeQueries.incrementAndGet();
        peakConcurrentQueries.accumulateAndGet(active, Math::max);

        long queryStart = System.nanoTime();

        try {
            return jdbcTemplate.query(
                    """
                    SELECT data_key, data_value
                    FROM items
                    WHERE data_key = ?
                    """,
                    rs -> {
                        if (rs.next()) {
                            return Optional.of(
                                    new Item(
                                            rs.getString("data_key"),
                                            rs.getString("data_value")
                                    )
                            );
                        }

                        return Optional.empty();
                    },
                    key
            );
        } finally {
            long queryNanos = System.nanoTime() - queryStart;

            totalQueryTimeNanos.addAndGet(queryNanos);
            maxQueryTimeNanos.accumulateAndGet(
                    queryNanos, Math::max);

            activeQueries.decrementAndGet();

            dbSemaphore.release();
            if (controllerEnabled) {
                controller.record(
                        queryNanos / 1_000_000.0,
                        waitNanos / 1_000_000.0
                );
            }
        }
    }

    public Map<String, Object> getQueryStats() {
        long queries = totalQueries.get();
        long waits = permitWaitCount.get();

        double avgQueryMs = queries == 0 ? 0.0
                : totalQueryTimeNanos.get()
                  / 1_000_000.0 / queries;

        double avgWaitMs = waits == 0 ? 0.0
                : totalPermitWaitNanos.get()
                  / 1_000_000.0 / waits;

        Map<String, Object> stats = new LinkedHashMap<>();

        stats.put("semaphoreLimit", dbSemaphore.getLimit());
        stats.put("activeQueries", activeQueries.get());
        stats.put("peakConcurrentQueries", peakConcurrentQueries.get());
        stats.put("totalQueries", queries);

        stats.put("avgQueryLatencyMs", avgQueryMs);
        stats.put("maxQueryLatencyMs",
                maxQueryTimeNanos.get() / 1_000_000.0);

        stats.put("permitWaitCount", waits);
        stats.put("avgPermitWaitMs", avgWaitMs);
        stats.put("maxPermitWaitMs",
                maxPermitWaitNanos.get() / 1_000_000.0);

        return stats;
    }

    public void resetQueryStats() {
        totalQueries.set(0);

        // Preserve the currently active count.
        peakConcurrentQueries.set(activeQueries.get());

        totalQueryTimeNanos.set(0);
        maxQueryTimeNanos.set(0);

        permitWaitCount.set(0);
        totalPermitWaitNanos.set(0);
        maxPermitWaitNanos.set(0);
    }

    public List<AdaptiveConcurrencyController.Decision> getControllerHistory() {
        return controller.getHistory();
    }

    public void resetControllerStats() {
        controller.resetHistory();
    }
}
