package org.apache.nifi.util.hive;

import java.util.Collections;
import java.util.Map;

public class DefaultHiveStorageHandlerConfig implements StorageHandlerConfig {

    public String getStorageHandlerClassName() {
        return null;
    }

    public Map<String, String> getTablePropertiesMap() {
        return Collections.emptyMap();
    }

}
