package com.catalyst.copy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
    private final boolean checksum;
    private final boolean restore;

    public FileCopyTask(Configuration conf, String hdfsPath, String localPath,
                         long expectedSize, int maxRetries, int bufferSize,
                         boolean checksum, boolean restore) {
        this.conf = conf;
        this.hdfsPath = hdfsPath;
        this.localPath = localPath;
        this.expectedSize = expectedSize;
        this.maxRetries = maxRetries;
        this.bufferSize = bufferSize;
        this.checksum = checksum;
        this.restore = restore;
    }

    @Override
    public Result call() {
        String thread = Thread.currentThread().getName();
        String label = restore ? localPath + " -> " + hdfsPath : hdfsPath;

        try {
            if (destinationExistsWithSize()) {
                LOG.info("[{}] [SKIP] size={}KB {}", thread, expectedSize / 1024, label);
                return new Result(Status.SKIPPED, 0);
            }
        } catch (IOException e) {
            LOG.warn("[{}] [SKIP-CHECK-FAILED] {}: {}", thread, label, e.getMessage());
        }

        int totalAttempts = maxRetries + 1;
        for (int attempt = 1; attempt <= totalAttempts; attempt++) {
            try {
                long start = System.currentTimeMillis();
                if (checksum) {
                    copyWithChecksum();
                } else {
                    copyDirect();
                }
                long elapsed = (System.currentTimeMillis() - start) / 1000;
                LOG.info("[{}] [OK] attempt={} elapsed={}s size={}KB  {}",
                        thread, attempt, elapsed, expectedSize / 1024, label);
                return new Result(Status.COPIED, expectedSize);
            } catch (Exception e) {
                cleanupTmp();
                if (attempt <= maxRetries) {
                    long backoffMs = (1L << attempt) * 1000;
                    LOG.warn("[{}] [RETRY {}/{}] {}: {}",
                            thread, attempt, maxRetries, label, e.getMessage());
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else {
                    LOG.error("[{}] [FAIL] {}: {}", thread, label, e.getMessage());
                    return new Result(Status.FAILED, 0);
                }
            }
        }
        return new Result(Status.FAILED, 0);
    }

    private boolean destinationExistsWithSize() throws IOException {
        if (restore) {
            Path dst = new Path(hdfsPath);
            FileSystem fs = FileSystem.newInstance(dst.toUri(), conf);
            try {
                return fs.exists(dst) && fs.getFileStatus(dst).getLen() == expectedSize;
            } finally {
                fs.close();
            }
        } else {
            File destFile = new File(localPath);
            return destFile.exists() && destFile.length() == expectedSize;
        }
    }

    private void copyDirect() throws IOException {
        FileSystem fs = null;
        try {
            InputStream in;
            OutputStream out;
            if (restore) {
                Path dst = new Path(hdfsPath);
                fs = FileSystem.newInstance(dst.toUri(), conf);
                if (dst.getParent() != null) {
                    fs.mkdirs(dst.getParent());
                }
                in = new BufferedInputStream(new FileInputStream(localPath), bufferSize);
                out = fs.create(dst, true, bufferSize);
            } else {
                Path src = new Path(hdfsPath);
                fs = FileSystem.newInstance(src.toUri(), conf);
                File destFile = new File(localPath);
                if (destFile.getParentFile() != null) {
                    destFile.getParentFile().mkdirs();
                }
                in = fs.open(src, bufferSize);
                out = new BufferedOutputStream(new FileOutputStream(destFile), bufferSize);
            }

            try {
                pipe(in, out);
            } finally {
                try { in.close(); } catch (IOException ignore) {}
                out.close();
            }
        } finally {
            if (fs != null) fs.close();
        }
    }

    private void copyWithChecksum() throws IOException, NoSuchAlgorithmException {
        FileSystem fs = null;
        MessageDigest writeDigest = MessageDigest.getInstance("MD5");
        String tmpLocalPath = localPath + ".tmp";
        String tmpHdfsPath = hdfsPath + ".tmp";

        try {
            InputStream in;
            OutputStream out;
            if (restore) {
                Path tmpDst = new Path(tmpHdfsPath);
                fs = FileSystem.newInstance(tmpDst.toUri(), conf);
                if (tmpDst.getParent() != null) {
                    fs.mkdirs(tmpDst.getParent());
                }
                in = new BufferedInputStream(new FileInputStream(localPath), bufferSize);
                out = fs.create(tmpDst, true, bufferSize);
            } else {
                Path src = new Path(hdfsPath);
                fs = FileSystem.newInstance(src.toUri(), conf);
                File tmpFile = new File(tmpLocalPath);
                if (tmpFile.getParentFile() != null) {
                    tmpFile.getParentFile().mkdirs();
                }
                in = fs.open(src, bufferSize);
                out = new BufferedOutputStream(new FileOutputStream(tmpFile), bufferSize);
            }

            try {
                pipeWithDigest(in, out, writeDigest);
            } finally {
                try { in.close(); } catch (IOException ignore) {}
                out.close();
            }

            String writeMd5 = toHex(writeDigest.digest());

            MessageDigest verifyDigest = MessageDigest.getInstance("MD5");
            InputStream verifyIn;
            if (restore) {
                Path tmpDst = new Path(tmpHdfsPath);
                verifyIn = fs.open(tmpDst, bufferSize);
            } else {
                verifyIn = new FileInputStream(tmpLocalPath);
            }
            try {
                digest(verifyIn, verifyDigest);
            } finally {
                verifyIn.close();
            }

            String verifyMd5 = toHex(verifyDigest.digest());
            if (!writeMd5.equals(verifyMd5)) {
                cleanupTmpInternal(fs);
                throw new IOException("MD5 mismatch after write: expected=" + writeMd5
                        + " actual=" + verifyMd5);
            }

            if (restore) {
                Path tmpDst = new Path(tmpHdfsPath);
                Path finalDst = new Path(hdfsPath);
                if (fs.exists(finalDst)) {
                    fs.delete(finalDst, false);
                }
                if (!fs.rename(tmpDst, finalDst)) {
                    fs.delete(tmpDst, false);
                    throw new IOException("HDFS rename failed: " + tmpDst + " -> " + finalDst);
                }
            } else {
                File tmpFile = new File(tmpLocalPath);
                File destFile = new File(localPath);
                if (destFile.exists()) {
                    destFile.delete();
                }
                if (!tmpFile.renameTo(destFile)) {
                    tmpFile.delete();
                    throw new IOException("Rename failed: " + tmpFile + " -> " + destFile);
                }
            }
        } finally {
            if (fs != null) fs.close();
        }
    }

    private void pipe(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[bufferSize];
        int n;
        while ((n = in.read(buf)) > 0) {
            out.write(buf, 0, n);
        }
        out.flush();
    }

    private void pipeWithDigest(InputStream in, OutputStream out, MessageDigest md)
            throws IOException {
        byte[] buf = new byte[bufferSize];
        int n;
        while ((n = in.read(buf)) > 0) {
            md.update(buf, 0, n);
            out.write(buf, 0, n);
        }
        out.flush();
    }

    private void digest(InputStream in, MessageDigest md) throws IOException {
        byte[] buf = new byte[bufferSize];
        int n;
        while ((n = in.read(buf)) > 0) {
            md.update(buf, 0, n);
        }
    }

    private void cleanupTmp() {
        if (restore) {
            try {
                Path tmpDst = new Path(hdfsPath + ".tmp");
                FileSystem fs = FileSystem.newInstance(tmpDst.toUri(), conf);
                try {
                    if (fs.exists(tmpDst)) fs.delete(tmpDst, false);
                } finally {
                    fs.close();
                }
            } catch (IOException ignore) {}
        } else {
            File tmpFile = new File(localPath + ".tmp");
            if (tmpFile.exists()) tmpFile.delete();
        }
    }

    private void cleanupTmpInternal(FileSystem fs) throws IOException {
        if (restore) {
            Path tmpDst = new Path(hdfsPath + ".tmp");
            if (fs.exists(tmpDst)) fs.delete(tmpDst, false);
        } else {
            File tmpFile = new File(localPath + ".tmp");
            if (tmpFile.exists()) tmpFile.delete();
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
