package org.apache.nifi.util.hive;


import java.util.Map;

public interface StorageHandlerConfig {

    String getStorageHandlerClassName();

    Map<String, String> getTablePropertiesMap();

}
