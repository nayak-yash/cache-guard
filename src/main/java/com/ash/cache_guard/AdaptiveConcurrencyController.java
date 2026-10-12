
package com.ash.cache_guard;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;

public class AdaptiveConcurrencyController {

    private static final int MIN_LIMIT = 2;
    private static final int MAX_LIMIT = 20;

    private static final int WINDOW_SIZE = 50;
    private static final int CALIBRATION_WINDOWS = 4;
    private static final int MAX_HISTORY = 100;

    private static final int HIGH_WINDOWS_TO_DECREASE = 2;
    private static final int HEALTHY_WINDOWS_TO_INCREASE = 3;

    private static final long COOLDOWN_NANOS = 1_000_000_000L;

    private static final double HIGH_LATENCY_FACTOR = 1.50;
    private static final double HEALTHY_LATENCY_FACTOR = 1.15;

    // Absolute safety guard. Make configurable after measuring
    // representative DB latency on your own environment.
    private static final double ABSOLUTE_P95_LIMIT_MS = 100.0;

    private static final double WAIT_THRESHOLD_MS = 1.0;
    private static final double MIN_WAIT_FRACTION_TO_INCREASE = 0.20;
    private static final double DECREASE_FACTOR = 0.70;

    private final AdjustableSemaphore semaphore;
    private final ArrayDeque<Decision> history = new ArrayDeque<>();
    private final double[] latencyWindow = new double[WINDOW_SIZE];
    private final double[] calibrationP95 =
            new double[CALIBRATION_WINDOWS];

    private int windowCount;
    private int waitedQueriesInWindow;
    private int sampleCount;
    private int completedWindows;

    private int calibrationCount;
    private boolean baselineInitialized;
    private double latencyBaselineMs;

    private int consecutiveHighLatencyWindows;
    private int consecutiveHealthyWindows;
    private long cooldownUntilNanos;

    public AdaptiveConcurrencyController(AdjustableSemaphore semaphore) {
        this.semaphore = semaphore;
    }

    public synchronized void record(
            double queryLatencyMs,
            double permitWaitMs) {

        if (!Double.isFinite(queryLatencyMs)
                || queryLatencyMs < 0
                || !Double.isFinite(permitWaitMs)
                || permitWaitMs < 0) {
            return;
        }

        latencyWindow[windowCount++] = queryLatencyMs;
        sampleCount++;

        if (permitWaitMs >= WAIT_THRESHOLD_MS) {
            waitedQueriesInWindow++;
        }

        if (windowCount == WINDOW_SIZE) {
            evaluateWindow();
        }
    }

    private void evaluateWindow() {
        double[] sorted = Arrays.copyOf(latencyWindow, windowCount);
        Arrays.sort(sorted);

        int p95Index = (int) Math.ceil(0.95 * sorted.length) - 1;
        double p95Ms = sorted[p95Index];

        double waitFraction =
                (double) waitedQueriesInWindow / windowCount;

        completedWindows++;

        int oldLimit = semaphore.getLimit();
        int newLimit = oldLimit;
        String reason;

        // Calibration: gather representative windows at the initial limit.
        if (completedWindows <= CALIBRATION_WINDOWS) {
            calibrationP95[calibrationCount++] = p95Ms;

            if (calibrationCount == CALIBRATION_WINDOWS) {
                double[] values = Arrays.copyOf(
                        calibrationP95, calibrationCount);
                Arrays.sort(values);

                // Median is more robust to a single slow startup window.
                latencyBaselineMs =
                        (values[1] + values[2]) / 2.0;
                baselineInitialized = true;
            }

            reason = baselineInitialized
                    ? "Calibration complete; baseline frozen"
                    : "Calibration: collecting baseline windows";

            addHistory(oldLimit, newLimit, p95Ms,
                    waitFraction, reason);
            clearWindow();
            return;
        }

        if (!baselineInitialized) {
            addHistory(oldLimit, newLimit, p95Ms,
                    waitFraction, "Waiting for calibrated baseline");
            clearWindow();
            return;
        }

        long now = System.nanoTime();

        boolean absoluteHigh = p95Ms > ABSOLUTE_P95_LIMIT_MS;
        boolean relativeHigh =
                p95Ms > latencyBaselineMs * HIGH_LATENCY_FACTOR;
        boolean highLatency = absoluteHigh || relativeHigh;

        boolean healthyLatency =
                p95Ms <= Math.min(
                        ABSOLUTE_P95_LIMIT_MS,
                        latencyBaselineMs * HEALTHY_LATENCY_FACTOR);

        boolean significantWait =
                waitFraction >= MIN_WAIT_FRACTION_TO_INCREASE;

        if (now < cooldownUntilNanos) {
            consecutiveHighLatencyWindows = 0;
            consecutiveHealthyWindows = 0;
            reason = "Cooldown: waiting for adjustment to settle";

        } else {
            if (highLatency) {
                consecutiveHighLatencyWindows++;
                consecutiveHealthyWindows = 0;
            } else if (healthyLatency && significantWait) {
                consecutiveHealthyWindows++;
                consecutiveHighLatencyWindows = 0;
            } else {
                consecutiveHighLatencyWindows = 0;
                consecutiveHealthyWindows = 0;
            }

            if (consecutiveHighLatencyWindows
                    >= HIGH_WINDOWS_TO_DECREASE) {

                newLimit = Math.max(
                        MIN_LIMIT,
                        (int) Math.floor(oldLimit * DECREASE_FACTOR));

                reason = newLimit < oldLimit
                        ? "Sustained high latency: multiplicative decrease"
                        : "High latency at minimum concurrency";

                consecutiveHighLatencyWindows = 0;
                consecutiveHealthyWindows = 0;

            } else if (consecutiveHealthyWindows
                    >= HEALTHY_WINDOWS_TO_INCREASE) {

                newLimit = Math.min(MAX_LIMIT, oldLimit + 1);

                reason = newLimit > oldLimit
                        ? "Healthy latency with queued demand: increase"
                        : "Already at maximum concurrency";

                consecutiveHealthyWindows = 0;
                consecutiveHighLatencyWindows = 0;

            } else if (highLatency) {
                reason = "High latency: awaiting confirmation";
            } else if (healthyLatency && significantWait) {
                reason = "Healthy window: awaiting more evidence";
            } else {
                reason = "Insufficient evidence to adjust concurrency";
            }
        }

        if (newLimit != oldLimit) {
            semaphore.resize(newLimit);
            cooldownUntilNanos = System.nanoTime() + COOLDOWN_NANOS;
        }

        addHistory(oldLimit, newLimit, p95Ms, waitFraction, reason);
        clearWindow();
    }

    private void addHistory(
            int oldLimit,
            int newLimit,
            double p95Ms,
            double waitFraction,
            String reason) {

        long remainingMs = Math.max(
                0L, cooldownUntilNanos - System.nanoTime()) / 1_000_000L;

        history.addLast(new Decision(
                Instant.now().toString(),
                oldLimit,
                newLimit,
                p95Ms,
                waitFraction,
                latencyBaselineMs,
                sampleCount,
                completedWindows,
                remainingMs,
                reason
        ));

        while (history.size() > MAX_HISTORY) {
            history.removeFirst();
        }
    }

    private void clearWindow() {
        windowCount = 0;
        waitedQueriesInWindow = 0;
    }

    public synchronized List<Decision> getHistory() {
        return List.copyOf(history);
    }

    public synchronized void resetHistory() {
        history.clear();
        Arrays.fill(latencyWindow, 0.0);
        Arrays.fill(calibrationP95, 0.0);

        windowCount = 0;
        waitedQueriesInWindow = 0;
        sampleCount = 0;
        completedWindows = 0;
        calibrationCount = 0;

        baselineInitialized = false;
        latencyBaselineMs = 0.0;

        consecutiveHighLatencyWindows = 0;
        consecutiveHealthyWindows = 0;
        cooldownUntilNanos = 0L;
    }

    public record Decision(
            String timestamp,
            int oldLimit,
            int newLimit,
            double windowP95LatencyMs,
            double permitWaitFraction,
            double latencyBaselineMs,
            int sampleCount,
            int completedWindows,
            long cooldownRemainingMs,
            String reason
    ) {}
}
