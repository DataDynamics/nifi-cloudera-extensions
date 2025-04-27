package org.apache.nifi.util.hive;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class IcebergStorageHandlerConfig implements StorageHandlerConfig {
    static final String ICEBERG_STORAGE_HANDLER_CLASSNAME = "ICEBERG";

    private final Map<String, String> tablePropertiesMap = new HashMap<>();

    public IcebergStorageHandlerConfig() {
        this.tablePropertiesMap.put("engine.hive.enabled", "true");
    }

    public String getStorageHandlerClassName() {
        return "ICEBERG";
    }

    public Map<String, String> getTablePropertiesMap() {
        return Collections.unmodifiableMap(this.tablePropertiesMap);
    }
}
