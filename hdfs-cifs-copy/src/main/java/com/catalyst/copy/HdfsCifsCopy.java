package com.catalyst.copy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsCifsCopy extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsCifsCopy.class);

    private static final String DEFAULT_SRC = "/user/catalyst/cff2.prod/{pid}/th";
    private static final String DEFAULT_DST = "/Catalyst_archive_data/backup_test/{pid}";
    private static final int DEFAULT_THREADS = 35;
    private static final int DEFAULT_RETRIES = 3;
    private static final int DEFAULT_BUFFER = 1048576;

    @Override
    public int run(String[] args) throws Exception {
        String pid = null;
        String src = null;
        String dst = null;
        int threads = DEFAULT_THREADS;
        int retries = DEFAULT_RETRIES;
        int buffer = DEFAULT_BUFFER;
        boolean checksum = true;
        String dbUrl = null;
        String dbUser = null;
        String dbPass = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--pid":         pid     = args[++i]; break;
                case "--src":         src     = args[++i]; break;
                case "--dst":         dst     = args[++i]; break;
                case "--threads":     threads = Integer.parseInt(args[++i]); break;
                case "--retries":     retries = Integer.parseInt(args[++i]); break;
                case "--buffer":      buffer  = Integer.parseInt(args[++i]); break;
                case "--no-checksum": checksum = false; break;
                case "--db-url":      dbUrl   = args[++i]; break;
                case "--db-user":     dbUser  = args[++i]; break;
                case "--db-pass":     dbPass  = args[++i]; break;
                default:
                    System.err.println("Unknown option: " + args[i]);
                    printUsage();
                    return 1;
            }
        }

        if (src == null && dst == null && pid == null) {
            System.err.println("Error: --pid is required when --src/--dst are not specified");
            printUsage();
            return 1;
        }

        if (src == null) src = DEFAULT_SRC;
        if (dst == null) dst = DEFAULT_DST;

        if (pid != null) {
            src = src.replace("{pid}", pid);
            dst = dst.replace("{pid}", pid);
        }

        boolean useLock = dbUrl != null && pid != null;
        if (dbUrl != null && pid == null) {
            LOG.warn("DB connection specified but --pid is missing; skipping lock");
        }

        LOG.info("Source   : {}", src);
        LOG.info("Dest     : {}", dst);
        LOG.info("Threads  : {}", threads);
        LOG.info("Retries  : {}", retries);
        LOG.info("Buffer   : {} bytes", buffer);
        LOG.info("Checksum : {}", checksum);
        LOG.info("DB lock  : {}", useLock);

        WorkflowLock lock = null;
        if (useLock) {
            lock = new WorkflowLock(dbUrl, dbUser, dbPass, pid);
            if (!lock.acquire()) {
                LOG.error("Cannot proceed — PID {} is locked by another process", pid);
                return 2;
            }
        }

        try {
            CopyEngine engine = new CopyEngine(getConf(), src, dst, threads, retries, buffer, checksum);
            CopyEngine.CopyResult result = engine.execute();

            LOG.info("Files copied  : {}", result.getFilesCopied());
            LOG.info("Files skipped : {}", result.getFilesSkipped());
            LOG.info("Files failed  : {}", result.getFilesFailed());
            LOG.info("Bytes copied  : {} MB", result.getBytesCopied() / (1024L * 1024L));
            LOG.info("Elapsed       : {} s", result.getElapsedSeconds());
            if (result.getElapsedSeconds() > 0) {
                double throughputMBs = (result.getBytesCopied() / (1024.0 * 1024.0))
                        / result.getElapsedSeconds();
                LOG.info("Throughput    : {} MB/s", String.format("%.2f", throughputMBs));
            }

            return result.getFilesFailed() > 0 ? 1 : 0;
        } finally {
            if (lock != null) {
                lock.release();
            }
        }
    }

    private static void printUsage() {
        System.err.println("Usage: hadoop jar hdfs-cifs-copy-1.0.0-fat.jar [--pid <id>]"
                + " [--src <hdfs-path>] [--dst <local-path>]"
                + " [--threads N] [--retries N] [--buffer N] [--no-checksum]"
                + " [--db-url <jdbc-url>] [--db-user <user>] [--db-pass <pass>]");
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new HdfsCifsCopy(), args);
        System.exit(exitCode);
    }
}
