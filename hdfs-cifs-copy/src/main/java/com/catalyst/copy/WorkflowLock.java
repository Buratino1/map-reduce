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
            "SELECT id, host_id, acquired_at FROM workflow_locks"
            + " WHERE name = ? AND cid = ? AND workflow = ?";
    private static final String INSERT_SQL =
            "INSERT INTO workflow_locks (name, cid, workflow, host_id, lock_type)"
            + " VALUES (?, ?, ?, ?, ?)";
    private static final String DELETE_SQL =
            "DELETE FROM workflow_locks WHERE name = ? AND cid = ? AND workflow = ?";

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

    public boolean acquire() {
        try (Connection conn = getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(CHECK_SQL)) {
                ps.setString(1, name);
                ps.setString(2, pid);
                ps.setString(3, workflow);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        LOG.error("PID {} is already locked: name={} workflow={} host={} since={}",
                                pid, name, workflow,
                                rs.getString("host_id"), rs.getTimestamp("acquired_at"));
                        return false;
                    }
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
                ps.setString(1, name);
                ps.setString(2, pid);
                ps.setString(3, workflow);
                ps.setString(4, hostId);
                ps.setString(5, lockType);
                ps.executeUpdate();
            }

            LOG.info("Lock acquired: name={} cid={} workflow={} host={}",
                    name, pid, workflow, hostId);
            return true;
        } catch (SQLException e) {
            LOG.error("Failed to acquire lock for PID {}: {}", pid, e.getMessage());
            return false;
        }
    }

    public void release() {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(DELETE_SQL)) {
            ps.setString(1, name);
            ps.setString(2, pid);
            ps.setString(3, workflow);
            int rows = ps.executeUpdate();
            if (rows > 0) {
                LOG.info("Lock released: name={} cid={} workflow={}", name, pid, workflow);
            } else {
                LOG.warn("No lock found to release: name={} cid={} workflow={}",
                        name, pid, workflow);
            }
        } catch (SQLException e) {
            LOG.error("Failed to release lock for PID {}: {}", pid, e.getMessage());
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(dbUrl, dbUser, dbPass);
    }

    private static String resolveHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
