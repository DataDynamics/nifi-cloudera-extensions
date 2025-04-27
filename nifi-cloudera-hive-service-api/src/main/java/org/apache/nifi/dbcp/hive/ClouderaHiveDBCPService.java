package org.apache.nifi.dbcp.hive;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.dbcp.DBCPService;

@Tags({"hive", "dbcp", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Provides Database Connection Pooling Service for Cloudera Hive. Connections can be asked from pool and returned after usage.")
public interface ClouderaHiveDBCPService extends DBCPService {

    String getConnectionURL();

}
