import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LoadTest {
    private static final String BASE_URL = "http://localhost:8080";
    private static final String RESULTS_FILE = "benchmark-step-load-results.csv";

    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .version(HttpClient.Version.HTTP_1_1)
            .build();

    private static final LongAdder successCount = new LongAdder();
    private static final LongAdder errorCount = new LongAdder();
    private static final LongAdder clientDroppedCount = new LongAdder();
    private static final LongAdder dispatchedCount = new LongAdder();
    private static final List<Long> httpLatenciesMs =
            Collections.synchronizedList(new ArrayList<>());
    private static final List<Long> dispatchLatenessMs =
            Collections.synchronizedList(new ArrayList<>());
    private static final AtomicInteger inFlight = new AtomicInteger();
    private static final AtomicInteger peakInFlight = new AtomicInteger();
    private static final AtomicInteger keyCounter = new AtomicInteger();
    private static final Object completionLock = new Object();

    private static volatile boolean stopping;
    private static volatile long phase2StartNanos;
    private static volatile long benchmarkStartNanos;

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            System.err.println("Usage: java LoadTest.java <distinct|same> " +
                    "<phase1Qps> <phase2Qps> <phaseSeconds> <maxInFlight> <label>");
            System.err.println("Example: java LoadTest.java distinct 100 400 15 100 fixed-20-step");
            System.exit(1);
        }

        String workload = args[0].toLowerCase();
        int phase1Qps = Integer.parseInt(args[1]);
        int phase2Qps = Integer.parseInt(args[2]);
        int phaseSeconds = Integer.parseInt(args[3]);
        int maxInFlight = Integer.parseInt(args[4]);
        String label = args[5];

        if (!workload.equals("distinct") && !workload.equals("same")) {
            throw new IllegalArgumentException("Workload must be 'distinct' or 'same'.");
        }
        if (phase1Qps <= 0 || phase2Qps <= 0 || phaseSeconds <= 0 || maxInFlight <= 0) {
            throw new IllegalArgumentException("QPS, phase seconds, and maxInFlight must be positive.");
        }

        String initialDbStats = get("/statistics/database");
        post("/statistics/reset-metrics");

        stopping = false;
        benchmarkStartNanos = System.nanoTime();
        long phaseDurationNanos = TimeUnit.SECONDS.toNanos(phaseSeconds);
        int phase1Scheduled = runPhase(workload, phase1Qps, phaseDurationNanos,
                maxInFlight, benchmarkStartNanos);
        phase2StartNanos = System.nanoTime();
        int phase2Scheduled = runPhase(workload, phase2Qps, phaseDurationNanos,
                maxInFlight, phase2StartNanos);

        stopping = true;
        waitForAllRequests();

        long benchmarkEndNanos = System.nanoTime();
        double offeredDurationSeconds = phaseSeconds * 2.0;
        double elapsedIncludingDrain = (benchmarkEndNanos - benchmarkStartNanos) / 1_000_000_000.0;
        int scheduled = phase1Scheduled + phase2Scheduled;

        String dbStats = get("/statistics/database");
        String cacheStats = get("/statistics/cache");
        String controllerStats = get("/statistics/controller");

        double httpAvg = average(httpLatenciesMs);
        double httpP95 = percentile(httpLatenciesMs, 0.95);
        double httpP99 = percentile(httpLatenciesMs, 0.99);
        double dispatchAvg = average(dispatchLatenessMs);
        double dispatchP95 = percentile(dispatchLatenessMs, 0.95);
        double dispatchP99 = percentile(dispatchLatenessMs, 0.99);

        double actualThroughput = successCount.sum() / offeredDurationSeconds;
        double completionThroughput = successCount.sum() / elapsedIncludingDrain;

        String row = String.join(",",
                csv(Instant.now().toString()),
                csv(label),
                csv(workload),
                String.valueOf(phase1Qps),
                String.valueOf(phase2Qps),
                String.valueOf(phaseSeconds),
                String.valueOf(maxInFlight),
                String.valueOf(scheduled),
                String.valueOf(dispatchedCount.sum()),
                String.valueOf(successCount.sum()),
                String.valueOf(errorCount.sum()),
                String.valueOf(clientDroppedCount.sum()),
                String.valueOf(round(elapsedIncludingDrain)),
                String.valueOf(round(actualThroughput)),
                String.valueOf(round(completionThroughput)),
                String.valueOf(round(httpAvg)),
                String.valueOf(round(httpP95)),
                String.valueOf(round(httpP99)),
                String.valueOf(round(dispatchAvg)),
                String.valueOf(round(dispatchP95)),
                String.valueOf(round(dispatchP99)),
                String.valueOf(peakInFlight.get()),
                String.valueOf(number(dbStats, "peakConcurrentQueries")),
                String.valueOf(number(dbStats, "totalQueries")),
                String.valueOf(number(dbStats, "avgDbLatencyMs")),
                String.valueOf(number(dbStats, "maxDbLatencyMs")),
                String.valueOf(number(dbStats, "permitWaitCount")),
                String.valueOf(number(dbStats, "avgPermitWaitMs")),
                String.valueOf(number(dbStats, "maxPermitWaitMs")),
                String.valueOf(number(cacheStats, "cacheHits")),
                String.valueOf(number(cacheStats, "cacheMisses")),
                String.valueOf(number(cacheStats, "cacheHitRate")),
                String.valueOf(number(cacheStats, "databaseLoadCount")),
                String.valueOf(number(dbStats, "finalSemaphoreLimit")),
                csv(controllerStats));

        String header = "timestamp,label,workload,phase1Qps,phase2Qps,phaseSeconds,maxInFlight," +
                "scheduledRequests,dispatchedRequests,success,httpErrors,clientDropped,elapsedIncludingDrainSeconds," +
                "successPerOfferedSecond,successPerElapsedSecond,httpAvgLatencyMs,httpP95LatencyMs," +
                "httpP99LatencyMs,dispatchLatenessAvgMs,dispatchLatenessP95Ms," +
                "dispatchLatenessP99Ms,peakInFlight,peakConcurrentQueries,totalDbQueries," +
                "avgDbLatencyMs,maxDbLatencyMs,permitWaitCount,avgPermitWaitMs,maxPermitWaitMs," +
                "cacheHits,cacheMisses,cacheHitRate,databaseLoadCount,finalSemaphoreLimit,controllerHistoryJson";

        boolean needsHeader = !Files.exists(Path.of(RESULTS_FILE)) ||
                Files.size(Path.of(RESULTS_FILE)) == 0;
        try (var writer = Files.newBufferedWriter(Path.of(RESULTS_FILE),
                StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.APPEND)) {
            if (needsHeader) {
                writer.write(header);
                writer.newLine();
            }
            writer.write(row);
            writer.newLine();
        }

        System.out.println("\n=== Async step-load benchmark ===");
        System.out.println("Phase 1: " + phase1Qps + " QPS for " + phaseSeconds + "s (" + phase1Scheduled + " scheduled)");
        System.out.println("Phase 2: " + phase2Qps + " QPS for " + phaseSeconds + "s (" + phase2Scheduled + " scheduled)");
        System.out.println("Scheduled requests: " + scheduled);
        System.out.println("Successes: " + successCount.sum());
        System.out.println("HTTP errors: " + errorCount.sum());
        System.out.println("Dropped at client in-flight cap: " + clientDroppedCount.sum());
        System.out.println("Requests dispatched: " + dispatchedCount.sum());
        System.out.printf("Success throughput / offered duration: %.2f req/s%n", actualThroughput);
        System.out.printf("Success throughput / elapsed incl. drain: %.2f req/s%n", completionThroughput);
        System.out.printf("HTTP latency avg / P95 / P99: %.2f / %.2f / %.2f ms%n", httpAvg, httpP95, httpP99);
        System.out.printf("Dispatch lateness avg / P95 / P99: %.2f / %.2f / %.2f ms%n",
                dispatchAvg, dispatchP95, dispatchP99);
        System.out.println("Peak in-flight requests: " + peakInFlight.get());
        System.out.println("DB stats: " + dbStats);
        System.out.println("CSV appended to: " + Path.of(RESULTS_FILE).toAbsolutePath());

        if (dispatchP95 > 10) {
            System.out.println("WARNING: P95 dispatch lateness exceeds 10 ms; the client may not be sustaining the target schedule.");
        }
        if (scheduled != phase1Qps * phaseSeconds + phase2Qps * phaseSeconds) {
            System.out.println("WARNING: scheduled request count differs from expected; inspect benchmark settings.");
        }
    }

    private static int runPhase(String workload, int qps, long durationNanos,
                                int maxInFlight, long phaseStartNanos) throws InterruptedException {
        long intervalNanos = Math.max(1L, 1_000_000_000L / qps);
        int scheduled = 0;

        while (true) {
            long targetNanos = phaseStartNanos + scheduled * intervalNanos;
            long offset = targetNanos - phaseStartNanos;
            if (offset >= durationNanos) {
                break;
            }

            long remaining = targetNanos - System.nanoTime();
            if (remaining > 0) {
                TimeUnit.NANOSECONDS.sleep(remaining);
            }

            long actualDispatchNanos = System.nanoTime();
            long latenessMs = Math.max(0L,
                    TimeUnit.NANOSECONDS.toMillis(actualDispatchNanos - targetNanos));
            dispatchLatenessMs.add(latenessMs);

            if (inFlight.get() >= maxInFlight) {
                clientDroppedCount.increment();
                scheduled++;
                continue;
            }

            String key = nextKey(workload);
            dispatchRequest(key, actualDispatchNanos);
            scheduled++;
        }
        return scheduled;
    }

    private static void dispatchRequest(String key, long dispatchStartNanos) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + "/items/" + key))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();

        dispatchedCount.increment();
        int active = inFlight.incrementAndGet();
        peakInFlight.accumulateAndGet(active, Math::max);

        CLIENT.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .whenComplete((response, failure) -> {
                    try {
                        long latencyMs = TimeUnit.NANOSECONDS.toMillis(
                                System.nanoTime() - dispatchStartNanos);
                        httpLatenciesMs.add(latencyMs);

                        if (failure != null) {
                            errorCount.increment();
                        } else if (response.statusCode() >= 200 && response.statusCode() < 300) {
                            successCount.increment();
                        } else {
                            errorCount.increment();
                        }
                    } finally {
                        inFlight.decrementAndGet();
                        synchronized (completionLock) {
                            completionLock.notifyAll();
                        }
                    }
                });
    }

    private static void waitForAllRequests() throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(5);
        synchronized (completionLock) {
            while (inFlight.get() > 0) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new IllegalStateException("Timed out waiting for in-flight requests to finish.");
                }
                TimeUnit.NANOSECONDS.timedWait(completionLock, remaining);
            }
        }
    }

    private static String nextKey(String workload) {
        if (workload.equals("same")) {
            return "merchant-1";
        }
        int index = 2000 + keyCounter.getAndIncrement();
        if (index > 9999) {
            throw new IllegalStateException("Distinct workload exhausted seeded keys; use fewer requests.");
        }
        return "merchant-" + index;
    }

    private static String get(String path) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + path))
                .timeout(Duration.ofSeconds(10))
                .GET().build();
        HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("GET " + path + " returned " + response.statusCode() + ": " + response.body());
        }
        return response.body();
    }

    private static String post(String path) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + path))
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("POST " + path + " returned " + response.statusCode() + ": " + response.body());
        }
        return response.body();
    }

    private static double number(String json, String key) {
        Pattern pattern = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*(-?\\d+(?:\\.\\d+)?(?:[Ee][+-]?\\d+)?)");
        Matcher matcher = pattern.matcher(json);
        return matcher.find() ? Double.parseDouble(matcher.group(1)) : 0.0;
    }

    private static double average(List<Long> values) {
        synchronized (values) {
            if (values.isEmpty()) return 0.0;
            double sum = 0;
            for (long value : values) sum += value;
            return sum / values.size();
        }
    }

    private static double percentile(List<Long> values, double p) {
        synchronized (values) {
            if (values.isEmpty()) return 0.0;
            List<Long> copy = new ArrayList<>(values);
            Collections.sort(copy);
            int index = Math.max(0, (int) Math.ceil(p * copy.size()) - 1);
            return copy.get(index);
        }
    }

    private static double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    private static String csv(String value) {
        if (value == null) return "\"\"";
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }
}
