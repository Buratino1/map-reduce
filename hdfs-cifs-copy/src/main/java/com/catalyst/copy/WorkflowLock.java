package com.catalyst.copy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class WorkflowLock {

    private static final Logger LOG = LoggerFactory.getLogger(WorkflowLock.class);

    private static final String CHECK_SQL =
            "SELECT id, host_id, workflow, acquired_at FROM workflow_locks"
            + " WHERE name = ? AND cid = ? AND lock_type = 'X'";
    private static final String COUNT_EXCLUSIVE_SQL =
            "SELECT COUNT(*) AS cnt FROM workflow_locks"
            + " WHERE name = ? AND cid = ? AND lock_type = 'X'";
    private static final String INSERT_SQL =
            "INSERT INTO workflow_locks (name, cid, workflow, host_id, lock_type)"
            + " VALUES (?, ?, ?, ?, ?)";
    private static final String DELETE_SQL =
            "DELETE FROM workflow_locks WHERE name = ? AND cid = ? AND workflow = ?";

    private static final int MAX_RETRIES = 2000;
    private static final long RETRY_INTERVAL_MS = 2 * 60 * 1000L;
    private static final long VERIFY_DELAY_MS = 20 * 1000L;

    private final String dbUrl;
    private final String dbUser;
    private final String dbPass;
    private final String name;
    private final String pid;
    private final String workflow;
    private final String lockType;
    private final String hostId;

    public WorkflowLock(String dbUrl, String dbUser, String dbPass,
                         String name, String pid, String workflow, String lockType) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPass = dbPass;
        this.name = name;
        this.pid = pid;
        this.workflow = workflow;
        this.lockType = lockType;
        this.hostId = resolveHost();
    }

    public boolean tryAcquire() {
        try {
            if (hasExclusiveLock()) {
                return false;
            }

            insertLock();
            LOG.info("Lock inserted: name={} cid={} workflow={} host={}, verifying in 20s...",
                    name, pid, workflow, hostId);

            Thread.sleep(VERIFY_DELAY_MS);

            int exclusiveCount = countExclusiveLocks();
            if (exclusiveCount > 1) {
                LOG.warn("Race detected: {} exclusive locks for name={} cid={}, releasing ours",
                        exclusiveCount, name, pid);
                deleteLock();
                return false;
            }

            LOG.info("Lock verified and acquired: name={} cid={} workflow={} host={}",
                    name, pid, workflow, hostId);
            return true;
        } catch (SQLException e) {
            LOG.error("DB error on tryAcquire for PID {}: {}", pid, e.getMessage());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public boolean acquire() {
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                if (hasExclusiveLock()) {
                    LOG.info("Waiting for lock [{}/{}]: name={} cid={}",
                            attempt, MAX_RETRIES, name, pid);
                    Thread.sleep(RETRY_INTERVAL_MS);
                    continue;
                }

                insertLock();
                LOG.info("Lock inserted: name={} cid={} workflow={} host={}, verifying in 20s...",
                        name, pid, workflow, hostId);

                Thread.sleep(VERIFY_DELAY_MS);

                int exclusiveCount = countExclusiveLocks();
                if (exclusiveCount > 1) {
                    LOG.warn("Race detected: {} exclusive locks found for name={} cid={}, releasing ours and retrying",
                            exclusiveCount, name, pid);
                    deleteLock();
                    Thread.sleep(RETRY_INTERVAL_MS);
                    continue;
                }

                LOG.info("Lock verified and acquired: name={} cid={} workflow={} host={}",
                        name, pid, workflow, hostId);
                return true;
            } catch (SQLException e) {
                LOG.error("DB error on lock attempt {}/{} for PID {}: {}",
                        attempt, MAX_RETRIES, pid, e.getMessage());
                try {
                    Thread.sleep(RETRY_INTERVAL_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        LOG.error("Failed to acquire lock after {} attempts for PID {}", MAX_RETRIES, pid);
        return false;
    }

    public void release() {
        try {
            deleteLock();
            LOG.info("Lock released: name={} cid={} workflow={}", name, pid, workflow);
        } catch (SQLException e) {
            LOG.error("Failed to release lock for PID {}: {}", pid, e.getMessage());
        }
    }

    private boolean hasExclusiveLock() throws SQLException {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(CHECK_SQL)) {
            ps.setString(1, name);
            ps.setString(2, pid);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    LOG.info("Exclusive lock held: name={} cid={} workflow={} host={} since={}",
                            name, pid, rs.getString("workflow"),
                            rs.getString("host_id"), rs.getTimestamp("acquired_at"));
                    return true;
                }
            }
        }
        return false;
    }

    private int countExclusiveLocks() throws SQLException {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(COUNT_EXCLUSIVE_SQL)) {
            ps.setString(1, name);
            ps.setString(2, pid);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt("cnt");
            }
        }
    }

    private void insertLock() throws SQLException {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
            ps.setString(1, name);
            ps.setString(2, pid);
            ps.setString(3, workflow);
            ps.setString(4, hostId);
            ps.setString(5, lockType);
            ps.executeUpdate();
        }
    }

    private void deleteLock() throws SQLException {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(DELETE_SQL)) {
            ps.setString(1, name);
            ps.setString(2, pid);
            ps.setString(3, workflow);
            ps.executeUpdate();
        }
    }

    private Connection getConnection() throws SQLException {
        String url = dbUrl;
        if (url != null && !url.contains("useSSL")) {
            url += url.contains("?") ? "&useSSL=false" : "?useSSL=false";
        }
        return DriverManager.getConnection(url, dbUser, dbPass);
    }

    private static String resolveHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
