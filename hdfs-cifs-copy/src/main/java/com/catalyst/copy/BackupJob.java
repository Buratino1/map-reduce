package com.catalyst.copy;

public class BackupJob {
    private String pid;
    private String lockName;
    private String src;
    private String dst;
    private int threads = 35;
    private int retries = 3;
    private int buffer = 1048576;
    private boolean checksum = true;

    public BackupJob() {}

    public BackupJob(String pid, String lockName, String src, String dst,
                      int threads, boolean checksum) {
        this.pid = pid;
        this.lockName = lockName;
        this.src = src;
        this.dst = dst;
        this.threads = threads;
        this.checksum = checksum;
    }

    public String getPid()       { return pid; }
    public void setPid(String p) { this.pid = p; }
    public String getLockName()  { return lockName; }
    public String getSrc()       { return src; }
    public String getDst()       { return dst; }
    public int getThreads()      { return threads; }
    public int getRetries()      { return retries; }
    public int getBuffer()       { return buffer; }
    public boolean isChecksum()  { return checksum; }

    @Override
    public String toString() {
        return "pid=" + pid + " lockName=" + lockName + " src=" + src + " dst=" + dst;
    }
}

