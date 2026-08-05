package com.catalyst.copy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CopyEngine {

    private static final Logger LOG = LoggerFactory.getLogger(CopyEngine.class);

    private final Configuration conf;
    private final String srcPath;
    private final String dstPath;
    private final int threads;
    private final int retries;
    private final int bufferSize;
    private final boolean checksum;
    private final boolean restore;

    public CopyEngine(Configuration conf, String srcPath, String dstPath,
                       int threads, int retries, int bufferSize, boolean checksum,
                       boolean restore) {
        this.conf = conf;
        this.srcPath = srcPath;
        this.dstPath = dstPath;
        this.threads = threads;
        this.retries = retries;
        this.bufferSize = bufferSize;
        this.checksum = checksum;
        this.restore = restore;
    }

    public CopyResult execute() throws IOException, InterruptedException {
        List<FileEntry> files = restore ? listLocalFiles() : listHdfsFiles();
        LOG.info("Found {} files to copy ({} mode)", files.size(), restore ? "restore" : "backup");

        if (files.isEmpty()) {
            return new CopyResult(0, 0, 0, 0, 0);
        }

        long startTime = System.currentTimeMillis();

        final AtomicInteger counter = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(threads, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "hdfs-copy-" + counter.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        });

        List<Future<FileCopyTask.Result>> futures = new ArrayList<>();
        for (FileEntry file : files) {
            String hdfsSide;
            String localSide;
            if (restore) {
                hdfsSide  = file.destPath;
                localSide = file.srcPath;
            } else {
                hdfsSide  = file.srcPath;
                localSide = file.destPath;
            }

            FileCopyTask task = new FileCopyTask(
                    conf, hdfsSide, localSide, file.size,
                    retries, bufferSize, checksum, restore);
            futures.add(pool.submit(task));
        }

        pool.shutdown();
        pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

        long elapsed = (System.currentTimeMillis() - startTime) / 1000;

        int copied = 0;
        int skipped = 0;
        int failed = 0;
        long bytesCopied = 0;

        for (Future<FileCopyTask.Result> f : futures) {
            try {
                FileCopyTask.Result r = f.get();
                switch (r.getStatus()) {
                    case COPIED:
                        copied++;
                        bytesCopied += r.getBytes();
                        break;
                    case SKIPPED:
                        skipped++;
                        break;
                    case FAILED:
                        failed++;
                        break;
                }
            } catch (ExecutionException e) {
                failed++;
                LOG.error("Unexpected error", e.getCause());
            }
        }

        return new CopyResult(copied, skipped, failed, bytesCopied, elapsed);
    }

    private List<FileEntry> listHdfsFiles() throws IOException {
        List<FileEntry> result = new ArrayList<>();
        Path src = new Path(srcPath);
        FileSystem fs = src.getFileSystem(conf);

        if (!fs.exists(src)) {
            LOG.error("Source HDFS path does not exist: {}", srcPath);
            return result;
        }

        String srcRoot = src.toUri().getPath();
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(src, true);
        while (it.hasNext()) {
            LocatedFileStatus f = it.next();
            String filePath = f.getPath().toUri().getPath();
            String relative = filePath.substring(srcRoot.length());
            result.add(new FileEntry(f.getPath().toString(), dstPath + relative, f.getLen()));
        }
        return result;
    }

    private List<FileEntry> listLocalFiles() {
        List<FileEntry> result = new ArrayList<>();
        File srcDir = new File(srcPath);
        if (!srcDir.exists()) {
            LOG.error("Source local path does not exist: {}", srcPath);
            return result;
        }
        walkLocal(srcDir, result);
        return result;
    }

    private void walkLocal(File dir, List<FileEntry> result) {
        File[] entries = dir.listFiles();
        if (entries == null) return;
        for (File f : entries) {
            if (f.isDirectory()) {
                walkLocal(f, result);
            } else {
                String abs = f.getAbsolutePath();
                String relative = abs.substring(srcPath.length());
                result.add(new FileEntry(abs, dstPath + relative, f.length()));
            }
        }
    }

    private static class FileEntry {
        final String srcPath;
        final String destPath;
        final long size;

        FileEntry(String srcPath, String destPath, long size) {
            this.srcPath = srcPath;
            this.destPath = destPath;
            this.size = size;
        }
    }

    public static class CopyResult {
        private final int filesCopied;
        private final int filesSkipped;
        private final int filesFailed;
        private final long bytesCopied;
        private final long elapsedSeconds;

        public CopyResult(int filesCopied, int filesSkipped, int filesFailed,
                           long bytesCopied, long elapsedSeconds) {
            this.filesCopied = filesCopied;
            this.filesSkipped = filesSkipped;
            this.filesFailed = filesFailed;
            this.bytesCopied = bytesCopied;
            this.elapsedSeconds = elapsedSeconds;
        }

        public int getFilesCopied()     { return filesCopied; }
        public int getFilesSkipped()    { return filesSkipped; }
        public int getFilesFailed()     { return filesFailed; }
        public long getBytesCopied()    { return bytesCopied; }
        public long getElapsedSeconds() { return elapsedSeconds; }
    }
}
