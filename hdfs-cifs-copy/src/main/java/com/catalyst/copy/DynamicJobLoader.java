package com.catalyst.copy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DynamicJobLoader {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicJobLoader.class);

    private static final String QUERY =
            "SELECT p.productionId, "
            + "CASE WHEN c.name = 'cffv2' THEN 'cffv2' ELSE 'cffv1' END AS type "
            + "FROM production_systems p "
            + "LEFT JOIN production_systems_conf c "
            + "  ON p.productionId = c.productionId AND c.name = 'cffv2' "
            + "WHERE p.available = 1";

    private static final String DST_ROOT = "/mnt/hadoop-backup/catalyst_hdfs_backup";

    private final String dbUrl;
    private final String dbUser;
    private final String dbPass;
    private final List<String> extraExtIds;
    private final DynamicConfig config;

    public DynamicJobLoader(String dbUrl, String dbUser, String dbPass,
                             List<String> extraExtIds, DynamicConfig config) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPass = dbPass;
        this.extraExtIds = extraExtIds;
        this.config = config != null ? config : new DynamicConfig();
    }

    public List<BackupJob> load() throws SQLException {
        List<BackupJob> jobs = new ArrayList<>();
        int cffv1Count = 0;
        int cffv2Count = 0;

        Map<String, String> typeOverrides = config.getTypes();

        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(QUERY);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String pid = rs.getString("productionId");
                String type = rs.getString("type");

                String override = typeOverrides.get(pid);
                if (override != null) {
                    String normalized = "CFF2".equalsIgnoreCase(override) ? "cffv2" : "cffv1";
                    if (!normalized.equals(type)) {
                        LOG.info("Type override for pid {}: {} -> {}", pid, type, normalized);
                    }
                    type = normalized;
                }

                if ("cffv2".equals(type)) {
                    jobs.add(cffv2MainJob(pid));
                    cffv2Count++;
                } else {
                    jobs.add(cffv1MainJob(pid, "db_mv"));
                    jobs.add(cffv1MainJob(pid, "db_lv"));
                    cffv1Count++;
                }
                jobs.add(clientsJob(pid));
                jobs.add(extJob(pid));
            }
        }
        for (String id : extraExtIds) {
            jobs.add(extJob(id));
        }

        List<BackupJob> extraJobs = config.getExtraJobs();
        for (BackupJob job : extraJobs) {
            if (job.getPid() == null || job.getPid().isEmpty()) {
                job.setPid(lastPathSegment(job.getSrc()));
            }
            jobs.add(job);
        }

        LOG.info("Loaded {} jobs: {} cffv1 systems, {} cffv2 systems, {} extra ext ids, {} extra jobs",
                jobs.size(), cffv1Count, cffv2Count, extraExtIds.size(), extraJobs.size());
        return jobs;
    }

    private static String lastPathSegment(String path) {
        if (path == null) return "unknown";
        String trimmed = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
        int idx = trimmed.lastIndexOf('/');
        return idx >= 0 ? trimmed.substring(idx + 1) : trimmed;
    }

    private BackupJob cffv2MainJob(String pid) {
        return new BackupJob(
                pid, "CFF2",
                "/user/catalyst/cff2.prod/" + pid,
                DST_ROOT + "/CFF2/" + pid,
                400, false);
    }

    private BackupJob cffv1MainJob(String pid, String folder) {
        return new BackupJob(
                pid, "CFF1",
                "/user/catalyst/v2.systems.prod/" + pid + "/" + folder,
                DST_ROOT + "/CFF1/" + pid + "/" + folder,
                200, true);
    }

    private BackupJob clientsJob(String pid) {
        return new BackupJob(
                pid, "CFF1",
                "/user/catalyst/v2.systems.prod/" + pid + "/clients",
                DST_ROOT + "/clients/" + pid + "/clients",
                100, true);
    }

    private BackupJob extJob(String pid) {
        return new BackupJob(
                pid, "CFF1",
                "/user/catalyst/impala.prod/" + pid + "/ext",
                DST_ROOT + "/ext/" + pid + "/ext",
                200, false);
    }

    private Connection getConnection() throws SQLException {
        String url = dbUrl;
        if (url != null && !url.contains("useSSL")) {
            url += url.contains("?") ? "&useSSL=false" : "?useSSL=false";
        }
        return DriverManager.getConnection(url, dbUser, dbPass);
    }
}
