package com.catalyst.copy;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public class DynamicConfig {
    private Map<String, String> types;
    private List<BackupJob> extraJobs;

    public Map<String, String> getTypes() {
        return types != null ? types : new HashMap<String, String>();
    }

    public List<BackupJob> getExtraJobs() {
        return extraJobs != null ? extraJobs : new ArrayList<BackupJob>();
    }
}
