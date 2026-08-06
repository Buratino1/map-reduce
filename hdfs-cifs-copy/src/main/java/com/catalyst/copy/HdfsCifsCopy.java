package com.catalyst.copy;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class HdfsCifsCopy extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsCifsCopy.class);

    private static final String DEFAULT_SRC = "/user/catalyst/cff2.prod/{pid}/th";
    private static final String DEFAULT_DST = "/Catalyst_archive_data/backup_test/{pid}";
    private static final int DEFAULT_THREADS = 35;
    private static final int DEFAULT_RETRIES = 3;
    private static final int DEFAULT_BUFFER = 1048576;
    private static final long JOBS_RETRY_INTERVAL_MS = 2 * 60 * 1000L;
    private static final String DEFAULT_RESTORE_PREFIX = "/restored";

    private String restorePrefix = DEFAULT_RESTORE_PREFIX;

    @Override
    public int run(String[] args) throws Exception {
        Properties props = loadProperties();

        String pid = null;
        String src = null;
        String dst = null;
        int threads = DEFAULT_THREADS;
        int retries = DEFAULT_RETRIES;
        int buffer = DEFAULT_BUFFER;
        boolean checksum = true;
        String dbUrl = props.getProperty("db.url");
        String dbUser = props.getProperty("db.user");
        String dbPass = props.getProperty("db.pass");
        String lockName = null;
        String jobsFile = null;
        boolean dynamic = false;
        boolean restore = false;
        Set<String> onlyPids = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--pid":         pid      = args[++i]; break;
                case "--src":         src      = args[++i]; break;
                case "--dst":         dst      = args[++i]; break;
                case "--threads":     threads  = Integer.parseInt(args[++i]); break;
                case "--retries":     retries  = Integer.parseInt(args[++i]); break;
                case "--buffer":      buffer   = Integer.parseInt(args[++i]); break;
                case "--no-checksum": checksum = false; break;
                case "--db-url":      dbUrl    = args[++i]; break;
                case "--db-user":     dbUser   = args[++i]; break;
                case "--db-pass":     dbPass   = args[++i]; break;
                case "--lock-name":   lockName = args[++i]; break;
                case "--jobs":        jobsFile = args[++i]; break;
                case "--dynamic":     dynamic  = true; break;
                case "--restore":     restore  = true; break;
                case "--only-pid":
                    if (onlyPids == null) onlyPids = new HashSet<>();
                    for (String p : args[++i].split(",")) {
                        p = p.trim();
                        if (!p.isEmpty()) onlyPids.add(p);
                    }
                    break;
                default:
                    System.err.println("Unknown option: " + args[i]);
                    printUsage();
                    return 1;
            }
        }

        restorePrefix = props.getProperty("restore.prefix", DEFAULT_RESTORE_PREFIX);

        LOG.info("Mode     : {}", restore ? "RESTORE (local -> HDFS)" : "BACKUP (HDFS -> local)");
        if (restore) {
            LOG.info("Restore prefix: {}", restorePrefix);
        }
        if (onlyPids != null) {
            LOG.info("Only PIDs: {}", onlyPids);
        }

        if (dynamic) {
            return runDynamicJobs(dbUrl, dbUser, dbPass, props, restore, onlyPids);
        }

        if (jobsFile != null) {
            return runMultiJob(jobsFile, dbUrl, dbUser, dbPass, props, restore, onlyPids);
        }

        return runSingleJob(pid, src, dst, threads, retries, buffer, checksum,
                dbUrl, dbUser, dbPass, lockName, props, restore);
    }

    private int runDynamicJobs(String dbUrl, String dbUser, String dbPass,
                                Properties props, boolean restore, Set<String> onlyPids)
            throws Exception {
        if (dbUrl == null) {
            System.err.println("Error: DB connection required for --dynamic mode");
            return 1;
        }
        LOG.info("Loading jobs dynamically from DB...");
        List<String> extraExtIds = new ArrayList<>();
        String extExtra = props.getProperty("ext.extra", "");
        for (String id : extExtra.split(",")) {
            id = id.trim();
            if (!id.isEmpty()) extraExtIds.add(id);
        }
        DynamicJobLoader loader = new DynamicJobLoader(dbUrl, dbUser, dbPass, extraExtIds);
        List<BackupJob> jobs = loader.load();
        jobs = filterByPid(jobs, onlyPids);
        if (restore) jobs = transformForRestore(jobs);
        return processJobs(jobs, dbUrl, dbUser, dbPass, props, restore);
    }

    private int runSingleJob(String pid, String src, String dst,
                              int threads, int retries, int buffer, boolean checksum,
                              String dbUrl, String dbUser, String dbPass,
                              String lockName, Properties props, boolean restore)
            throws Exception {
        if (pid == null) {
            System.err.println("Error: --pid is required");
            printUsage();
            return 1;
        }

        if (src == null) src = DEFAULT_SRC;
        if (dst == null) dst = DEFAULT_DST;

        src = src.replace("{pid}", pid);
        dst = dst.replace("{pid}", pid);

        String actualSrc = src;
        String actualDst = dst;
        if (restore) {
            actualSrc = dst;
            actualDst = restorePrefix + src;
        }

        boolean useLock = dbUrl != null && !restore;

        LOG.info("PID      : {}", pid);
        LOG.info("Source   : {}", actualSrc);
        LOG.info("Dest     : {}", actualDst);
        LOG.info("Threads  : {}", threads);
        LOG.info("Retries  : {}", retries);
        LOG.info("Buffer   : {} bytes", buffer);
        LOG.info("Checksum : {}", checksum);
        LOG.info("DB lock  : {}", useLock);

        WorkflowLock lock = null;
        if (useLock) {
            if (lockName == null) lockName = props.getProperty("lock.name");
            if (lockName == null) {
                System.err.println("Error: --lock-name is required (e.g. CFF1, CFF2)");
                printUsage();
                return 1;
            }
            String lockWorkflow = props.getProperty("lock.workflow", "hdfs-cifs-copy");
            String lockType     = props.getProperty("lock.type", "X");
            LOG.info("Lock     : name={} cid={} workflow={}", lockName, pid, lockWorkflow);
            lock = new WorkflowLock(dbUrl, dbUser, dbPass, lockName, pid, lockWorkflow, lockType);
            if (!lock.acquire()) {
                LOG.error("Cannot proceed — PID {} is locked by another process", pid);
                return 2;
            }
        }

        try {
            return executeCopy(actualSrc, actualDst, threads, retries, buffer, checksum, restore);
        } finally {
            if (lock != null) {
                lock.release();
            }
        }
    }

    private int runMultiJob(String jobsFile, String dbUrl, String dbUser,
                             String dbPass, Properties props, boolean restore,
                             Set<String> onlyPids) throws Exception {
        if (dbUrl == null) {
            System.err.println("Error: DB connection required for --jobs mode (configure in application.properties)");
            return 1;
        }

        List<BackupJob> jobs;
        Type listType = new TypeToken<List<BackupJob>>() {}.getType();
        try (InputStreamReader reader = new InputStreamReader(
                new FileInputStream(jobsFile), StandardCharsets.UTF_8)) {
            jobs = new Gson().fromJson(reader, listType);
        }

        LOG.info("Loaded {} jobs from {}", jobs.size(), jobsFile);
        jobs = filterByPid(jobs, onlyPids);
        if (restore) jobs = transformForRestore(jobs);
        return processJobs(jobs, dbUrl, dbUser, dbPass, props, restore);
    }

    private List<BackupJob> filterByPid(List<BackupJob> jobs, Set<String> onlyPids) {
        if (onlyPids == null || onlyPids.isEmpty()) return jobs;
        List<BackupJob> filtered = new ArrayList<>();
        for (BackupJob j : jobs) {
            if (onlyPids.contains(j.getPid())) filtered.add(j);
        }
        LOG.info("Filtered {} jobs down to {} for PIDs={}",
                jobs.size(), filtered.size(), onlyPids);
        return filtered;
    }

    private List<BackupJob> transformForRestore(List<BackupJob> jobs) {
        List<BackupJob> restored = new ArrayList<>();
        for (BackupJob j : jobs) {
            String newSrc = j.getDst();
            String newDst = restorePrefix + j.getSrc();
            restored.add(new BackupJob(
                    j.getPid(), j.getLockName(),
                    newSrc, newDst,
                    j.getThreads(), j.isChecksum()));
        }
        return restored;
    }

    private int processJobs(List<BackupJob> jobs, String dbUrl, String dbUser,
                             String dbPass, Properties props, boolean restore) throws Exception {
        for (int i = 0; i < jobs.size(); i++) {
            LOG.info("  Job {}: {}", i + 1, jobs.get(i));
        }

        String lockWorkflow = props.getProperty("lock.workflow", "hdfs-cifs-copy");
        String lockType     = props.getProperty("lock.type", "X");

        List<BackupJob> pending = new ArrayList<>(jobs);
        List<BackupJob> failed = new ArrayList<>();
        int totalCompleted = 0;

        while (!pending.isEmpty()) {
            List<BackupJob> stillPending = new ArrayList<>();

            for (Iterator<BackupJob> it = pending.iterator(); it.hasNext(); ) {
                BackupJob job = it.next();
                LOG.info("--- Trying job: pid={} lockName={} src={} ---",
                        job.getPid(), job.getLockName(), job.getSrc());

                WorkflowLock lock = null;
                if (!restore) {
                    lock = new WorkflowLock(
                            dbUrl, dbUser, dbPass,
                            job.getLockName(), job.getPid(), lockWorkflow, lockType);

                    if (!lock.tryAcquire()) {
                        LOG.info("PID {} is locked, skipping for now", job.getPid());
                        stillPending.add(job);
                        continue;
                    }
                }

                try {
                    int rc = executeCopy(
                            job.getSrc(), job.getDst(),
                            job.getThreads(), job.getRetries(),
                            job.getBuffer(), job.isChecksum(), restore);
                    if (rc == 0) {
                        totalCompleted++;
                        LOG.info("--- Job completed: pid={} src={} ---",
                                job.getPid(), job.getSrc());
                    } else {
                        failed.add(job);
                        LOG.error("--- Job finished with errors: pid={} src={} ---",
                                job.getPid(), job.getSrc());
                    }
                } catch (Exception e) {
                    failed.add(job);
                    LOG.error("--- Job failed: pid={}: {} ---", job.getPid(), e.getMessage());
                } finally {
                    if (lock != null) {
                        lock.release();
                    }
                }
            }

            if (!stillPending.isEmpty()) {
                LOG.info("Waiting 2 minutes before retrying {} remaining jobs...",
                        stillPending.size());
                Thread.sleep(JOBS_RETRY_INTERVAL_MS);
            }
            pending = stillPending;
        }

        LOG.info("=== All jobs processed ===");
        LOG.info("Completed : {}", totalCompleted);
        LOG.info("Failed    : {}", failed.size());
        for (BackupJob f : failed) {
            LOG.info("  FAILED: {}", f);
        }

        return failed.isEmpty() ? 0 : 1;
    }

    private int executeCopy(String src, String dst, int threads, int retries,
                             int buffer, boolean checksum, boolean restore)
            throws IOException, InterruptedException {
        LOG.info("Source   : {}", src);
        LOG.info("Dest     : {}", dst);
        LOG.info("Threads  : {}", threads);
        LOG.info("Checksum : {}", checksum);

        CopyEngine engine = new CopyEngine(getConf(), src, dst, threads, retries,
                buffer, checksum, restore);
        CopyEngine.CopyResult result = engine.execute();

        LOG.info("Files copied  : {}", result.getFilesCopied());
        LOG.info("Files skipped : {}", result.getFilesSkipped());
        LOG.info("Files failed  : {}", result.getFilesFailed());
        LOG.info("Bytes copied  : {}", formatBytes(result.getBytesCopied()));
        long secs = result.getElapsedSeconds();
        LOG.info("Elapsed       : {} ({} s)",
                String.format("%d:%02d:%02d", secs / 3600, (secs % 3600) / 60, secs % 60),
                secs);
        if (result.getElapsedSeconds() > 0) {
            double throughputMBs = (result.getBytesCopied() / (1024.0 * 1024.0))
                    / result.getElapsedSeconds();
            LOG.info("Throughput    : {} MB/s", String.format("%.2f", throughputMBs));
        }

        return result.getFilesFailed() > 0 ? 1 : 0;
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024L) {
            return bytes + " B";
        } else if (bytes < 1024L * 1024) {
            return String.format("%,.2f KB", bytes / 1024.0);
        } else if (bytes < 1024L * 1024 * 1024) {
            return String.format("%,.2f MB", bytes / (1024.0 * 1024));
        } else if (bytes < 1024L * 1024 * 1024 * 1024) {
            return String.format("%,.2f GB", bytes / (1024.0 * 1024 * 1024));
        } else {
            return String.format("%,.2f TB", bytes / (1024.0 * 1024 * 1024 * 1024));
        }
    }

    private static Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream in = HdfsCifsCopy.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (in != null) {
                props.load(in);
            }
        } catch (IOException e) {
            LOG.warn("Could not load application.properties: {}", e.getMessage());
        }
        return props;
    }

    private static void printUsage() {
        System.err.println("Usage:");
        System.err.println("  Single job:  hadoop jar hdfs-cifs-copy-1.0.0-fat.jar --pid <id>"
                + " [--src <hdfs-path>] [--dst <local-path>]"
                + " [--threads N] [--retries N] [--buffer N] [--no-checksum]"
                + " [--lock-name <CFF1|CFF2>] [--restore]");
        System.err.println("  Multi job:   hadoop jar hdfs-cifs-copy-1.0.0-fat.jar --jobs <file.json>"
                + " [--restore] [--only-pid <id1,id2,...>]");
        System.err.println("  Dynamic:     hadoop jar hdfs-cifs-copy-1.0.0-fat.jar --dynamic"
                + " [--restore] [--only-pid <id1,id2,...>]");
        System.err.println();
        System.err.println("  --restore   reverses copy direction: reads from local dst,");
        System.err.println("              writes to HDFS at /restored + original src path");
        System.err.println("  --only-pid  process only listed production IDs;");
        System.err.println("              accepts comma-separated (--only-pid 7001,2020) or");
        System.err.println("              repeated (--only-pid 7001 --only-pid 2020)");
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new HdfsCifsCopy(), args);
        System.exit(exitCode);
    }
}
