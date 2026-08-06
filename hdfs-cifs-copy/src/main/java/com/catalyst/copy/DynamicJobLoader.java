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

    public DynamicJobLoader(String dbUrl, String dbUser, String dbPass) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPass = dbPass;
    }

    public List<BackupJob> load() throws SQLException {
        List<BackupJob> jobs = new ArrayList<>();
        int cffv1Count = 0;
        int cffv2Count = 0;

        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(QUERY);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String pid = rs.getString("productionId");
                String type = rs.getString("type");
                if ("cffv2".equals(type)) {
                    jobs.add(cffv2MainJob(pid));
                    cffv2Count++;
                } else {
                    jobs.add(cffv1MainJob(pid));
                    cffv1Count++;
                }
                jobs.add(clientsJob(pid));
                jobs.add(extJob(pid));
            }
        }
        LOG.info("Loaded {} jobs from DB: {} cffv1 systems, {} cffv2 systems",
                jobs.size(), cffv1Count, cffv2Count);
        return jobs;
    }

    private BackupJob cffv2MainJob(String pid) {
        return new BackupJob(
                pid, "CFF2",
                "/user/catalyst/cff2.prod/" + pid,
                DST_ROOT + "/CFF2/" + pid,
                400, false);
    }

    private BackupJob cffv1MainJob(String pid) {
        return new BackupJob(
                pid, "CFF1",
                "/user/catalyst/v2.systems.prod/" + pid + "/db_mv",
                DST_ROOT + "/CFF1/" + pid + "/db_mv",
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
