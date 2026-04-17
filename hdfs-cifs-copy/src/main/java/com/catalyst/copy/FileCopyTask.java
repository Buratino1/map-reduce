package com.catalyst.copy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Callable;

public class FileCopyTask implements Callable<FileCopyTask.Result> {

    private static final Logger LOG = LoggerFactory.getLogger(FileCopyTask.class);

    private final Configuration conf;
    private final String hdfsPath;
    private final String localPath;
    private final long expectedSize;
    private final int maxRetries;
    private final int bufferSize;

    public FileCopyTask(Configuration conf, String hdfsPath, String localPath,
                         long expectedSize, int maxRetries, int bufferSize) {
        this.conf = conf;
        this.hdfsPath = hdfsPath;
        this.localPath = localPath;
        this.expectedSize = expectedSize;
        this.maxRetries = maxRetries;
        this.bufferSize = bufferSize;
    }

    @Override
    public Result call() {
        String thread = Thread.currentThread().getName();
        File destFile = new File(localPath);
        if (destFile.exists() && destFile.length() == expectedSize) {
            LOG.info("[{}] [SKIP] size={}KB {}", thread, expectedSize / 1024, hdfsPath);
            return new Result(Status.SKIPPED, 0);
        }

        int totalAttempts = maxRetries + 1;
        for (int attempt = 1; attempt <= totalAttempts; attempt++) {
            try {
                long start = System.currentTimeMillis();
                copyWithChecksum();
                long elapsed = (System.currentTimeMillis() - start) / 1000;
                LOG.info("[{}] [OK] attempt={} elapsed={}s size={}KB  {}",
                        thread, attempt, elapsed, expectedSize / 1024, hdfsPath);
                return new Result(Status.COPIED, expectedSize);
            } catch (Exception e) {
                if (attempt <= maxRetries) {
                    long backoffMs = (1L << attempt) * 1000;
                    LOG.warn("[{}] [RETRY {}/{}] {}: {}",
                            thread, attempt, maxRetries, hdfsPath, e.getMessage());
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else {
                    LOG.error("[{}] [FAIL] {}: {}", thread, hdfsPath, e.getMessage());
                    return new Result(Status.FAILED, 0);
                }
            }
        }
        return new Result(Status.FAILED, 0);
    }

    private void copyWithChecksum() throws IOException, NoSuchAlgorithmException {
        Path src = new Path(hdfsPath);
        FileSystem fs = FileSystem.newInstance(src.toUri(), conf);
        try {

        File destFile = new File(localPath);
        File tmpFile = new File(localPath + ".tmp");
        destFile.getParentFile().mkdirs();

        MessageDigest writeDigest = MessageDigest.getInstance("MD5");

        try (FSDataInputStream in = fs.open(src, bufferSize);
             BufferedOutputStream out = new BufferedOutputStream(
                     new FileOutputStream(tmpFile), bufferSize)) {
            byte[] buf = new byte[bufferSize];
            int n;
            while ((n = in.read(buf)) > 0) {
                writeDigest.update(buf, 0, n);
                out.write(buf, 0, n);
            }
            out.flush();
        }

        String writeMd5 = toHex(writeDigest.digest());

        MessageDigest verifyDigest = MessageDigest.getInstance("MD5");
        try (FileInputStream in = new FileInputStream(tmpFile)) {
            byte[] buf = new byte[bufferSize];
            int n;
            while ((n = in.read(buf)) > 0) {
                verifyDigest.update(buf, 0, n);
            }
        }

        String verifyMd5 = toHex(verifyDigest.digest());

        if (!writeMd5.equals(verifyMd5)) {
            tmpFile.delete();
            throw new IOException("MD5 mismatch after write: expected=" + writeMd5
                    + " actual=" + verifyMd5);
        }

        if (destFile.exists()) {
            destFile.delete();
        }
        if (!tmpFile.renameTo(destFile)) {
            tmpFile.delete();
            throw new IOException("Rename failed: " + tmpFile + " -> " + destFile);
        }
        } finally {
            fs.close();
        }
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }

    public enum Status { COPIED, SKIPPED, FAILED }

    public static class Result {
        private final Status status;
        private final long bytes;

        public Result(Status status, long bytes) {
            this.status = status;
            this.bytes = bytes;
        }

        public Status getStatus() { return status; }
        public long getBytes()    { return bytes; }
    }
}
