import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

public class LogAnalyzer {

    // ---------- Configurable defaults ----------
    private static final int DEFAULT_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private static final List<String> DEFAULT_KEYWORDS = Arrays.asList("ERROR", "WARN", "INFO", "DEBUG", "Exception", "failed");
    private static final String OUTPUT_FILENAME = "analysis_result.txt";

    // ---------- Main entry ----------
    public static void main(String[] args) throws Exception {
        // Parse args
        Path logsFolder = null;
        int threadPoolSize = DEFAULT_POOL_SIZE;
        List<String> keywords = new ArrayList<>(DEFAULT_KEYWORDS);

        if (args.length >= 1) {
            logsFolder = Paths.get(args[0]);
        }
        if (args.length >= 2) {
            try { threadPoolSize = Integer.parseInt(args[1]); } catch (NumberFormatException e) { /* ignore, keep default */ }
        }
        if (args.length >= 3) {
            keywords = Arrays.stream(args[2].split(","))
                             .map(String::trim)
                             .filter(s -> !s.isEmpty())
                             .collect(Collectors.toList());
            if (keywords.isEmpty()) keywords = new ArrayList<>(DEFAULT_KEYWORDS);
        }

        // If no folder provided or folder doesn't exist, generate demo files in ./demo_logs
        if (logsFolder == null || !Files.exists(logsFolder)) {
            logsFolder = Paths.get("demo_logs");
            if (!Files.exists(logsFolder)) Files.createDirectories(logsFolder);
            System.out.println("No valid log folder provided. Generating demo logs in " + logsFolder.toAbsolutePath());
            generateDemoLogs(logsFolder, 8, 2000); // 8 files, 2000 lines each (configurable)
        }

        System.out.println("Analyzing folder: " + logsFolder.toAbsolutePath());
        System.out.println("Thread pool size: " + threadPoolSize);
        System.out.println("Keywords: " + keywords);
        System.out.println();

        // Collect log files
        List<Path> logFiles;
        try (Stream<Path> s = Files.walk(logsFolder)) {
            logFiles = s.filter(Files::isRegularFile)
                        .filter(p -> {
                            String name = p.getFileName().toString().toLowerCase();
                            return name.endsWith(".log") || name.endsWith(".txt");
                        })
                        .collect(Collectors.toList());
        }

        if (logFiles.isEmpty()) {
            System.out.println("No .log or .txt files found in " + logsFolder);
            return;
        }

        // Sequential run for baseline
        Map<String, Long> seqResult = new ConcurrentHashMap<>();
        long seqStart = System.nanoTime();
        sequentialAnalyze(logFiles, keywords, seqResult);
        long seqEnd = System.nanoTime();

        // Concurrent run with thread pool and Callable tasks
        ConcurrentHashMap<String, Long> concurrentResult = new ConcurrentHashMap<>();
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
        List<Future<Map<String, Long>>> futures = new ArrayList<>();

        // Submit tasks
        long concStart = System.nanoTime();
        for (Path file : logFiles) {
            futures.add(executor.submit(new FileAnalyzerCallable(file, keywords)));
        }

        // Optional simple "monitoring" loop that prints pool stats
        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
        monitor.scheduleAtFixedRate(() -> {
            if (executor instanceof ThreadPoolExecutor) {
                ThreadPoolExecutor tpe = (ThreadPoolExecutor) executor;
                System.out.println(String.format("[POOL] Active=%d, Completed=%d, TaskCount=%d, LargestPool=%d",
                        tpe.getActiveCount(), tpe.getCompletedTaskCount(), tpe.getTaskCount(), tpe.getLargestPoolSize()));
            }
        }, 0, 500, TimeUnit.MILLISECONDS);

        // Collect results and merge into concurrentResult
        for (Future<Map<String, Long>> f : futures) {
            try {
                Map<String, Long> perFile = f.get(); // wait for file's analysis
                perFile.forEach((k, v) -> concurrentResult.merge(k, v, Long::sum));
            } catch (ExecutionException ee) {
                System.err.println("Task failed: " + ee.getMessage());
            }
        }

        long concEnd = System.nanoTime();
        executor.shutdown();
        monitor.shutdownNow();

        // Output and write to file
        String summary = buildSummary(logFiles, keywords, seqResult, seqStart, seqEnd, concurrentResult, concStart, concEnd);
        System.out.println(summary);
        writeToFile(OUTPUT_FILENAME, summary);

        System.out.println("Summary written to " + Paths.get(OUTPUT_FILENAME).toAbsolutePath());
        System.out.println("Done.");
    }


    // ---------- Callable that analyzes a single file ----------
    static class FileAnalyzerCallable implements Callable<Map<String, Long>> {
        private final Path file;
        private final List<String> keywords;

        FileAnalyzerCallable(Path file, List<String> keywords) {
            this.file = file;
            this.keywords = keywords;
        }

        @Override
        public Map<String, Long> call() throws Exception {
            Map<String, Long> counts = new HashMap<>();
            keywords.forEach(k -> counts.put(k, 0L));

            // Use Streams to read and process lines
            // For each line, check occurrences of each keyword (simple contains-based count)
            try (Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8)) {
                lines.forEach(line -> {
                    for (String kw : keywords) {
                        // Count occurrences of keyword in the line (non-overlapping)
                        long occ = countOccurrences(line, kw);
                        if (occ > 0) counts.merge(kw, occ, Long::sum);
                    }
                });
            } catch (IOException ioe) {
                System.err.println("Failed to read " + file + ": " + ioe.getMessage());
            }

            // Simulated work log for monitoring
            System.out.println("[TASK] " + Thread.currentThread().getName() + " processed " + file.getFileName());
            return counts;
        }

        // Count non-overlapping occurrences of substring in text (case-sensitive)
        private long countOccurrences(String text, String sub) {
            if (sub.isEmpty() || text.isEmpty()) return 0;
            long count = 0;
            int idx = 0;
            while ((idx = text.indexOf(sub, idx)) != -1) {
                count++;
                idx += sub.length();
            }
            return count;
        }
    }


    // ---------- Sequential analysis used for baseline ----------
    private static void sequentialAnalyze(List<Path> files, List<String> keywords, Map<String, Long> outMap) {
        keywords.forEach(k -> outMap.put(k, 0L));
        for (Path file : files) {
            try (Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8)) {
                lines.forEach(line -> {
                    for (String kw : keywords) {
                        long occ = countOccurrencesStatic(line, kw);
                        if (occ > 0) outMap.merge(kw, occ, Long::sum);
                    }
                });
                System.out.println("[SEQ] processed " + file.getFileName());
            } catch (IOException e) {
                System.err.println("Failed to read sequentially " + file + ": " + e.getMessage());
            }
        }
    }

    private static long countOccurrencesStatic(String text, String sub) {
        if (sub.isEmpty() || text.isEmpty()) return 0;
        long count = 0;
        int idx = 0;
        while ((idx = text.indexOf(sub, idx)) != -1) {
            count++;
            idx += sub.length();
        }
        return count;
    }


    // ---------- Summary builder ----------
    private static String buildSummary(List<Path> files, List<String> keywords,
                                       Map<String, Long> seqResult, long seqStart, long seqEnd,
                                       Map<String, Long> concResult, long concStart, long concEnd) {

        StringBuilder sb = new StringBuilder();
        sb.append("===== Multi-Threaded Log Analyzer Summary =====\n");
        sb.append("Files analyzed: ").append(files.size()).append("\n");
        sb.append("Keywords: ").append(keywords).append("\n\n");

        sb.append("--- Sequential Result ---\n");
        seqResult.forEach((k, v) -> sb.append(String.format("%-10s : %d\n", k, v)));
        long seqTimeNs = seqEnd - seqStart;
        sb.append(String.format("Sequential time: %d ns (%.3f ms)\n\n", seqTimeNs, seqTimeNs / 1_000_000.0));

        sb.append("--- Concurrent Result ---\n");
        concResult.forEach((k, v) -> sb.append(String.format("%-10s : %d\n", k, v)));
        long concTimeNs = concEnd - concStart;
        sb.append(String.format("Concurrent time: %d ns (%.3f ms)\n\n", concTimeNs, concTimeNs / 1_000_000.0));

        double speedup = (seqTimeNs > 0) ? ((double) seqTimeNs / concTimeNs) : Double.NaN;
        sb.append(String.format("Speedup (seq / concurrent): %.3f\n", speedup));
        sb.append("===============================================\n");
        return sb.toString();
    }

    // ---------- Utility to write summary file ----------
    private static void writeToFile(String filename, String content) {
        try {
            Files.write(Paths.get(filename), content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            System.err.println("Failed to write result file: " + e.getMessage());
        }
    }

    // ---------- Demo log file generator ----------
    private static void generateDemoLogs(Path folder, int fileCount, int linesPerFile) {
        Random rnd = new Random(12345);
        String[] choices = new String[]{
                "INFO User logged in successfully",
                "WARN Disk usage at 85%",
                "ERROR Unable to connect to DB",
                "DEBUG Cache miss for key: user_123",
                "Exception in thread main java.lang.NullPointerException",
                "Transaction failed for id 9988",
                "INFO Background job completed"
        };

        try {
            for (int f = 1; f <= fileCount; f++) {
                Path file = folder.resolve(String.format("demo_%02d.log", f));
                try (BufferedWriter bw = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
                    for (int i = 0; i < linesPerFile; i++) {
                        String line = choices[rnd.nextInt(choices.length)];
                        // sprinkle some additional random words
                        line = String.format("%s | user=%d | session=%d | msg=random-%d", line, rnd.nextInt(1000), rnd.nextInt(100000), rnd.nextInt(1000000));
                        bw.write(line);
                        bw.newLine();
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
