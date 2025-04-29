package io.datadynamics.nifi.dbcp.hive;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;

/**
 * Definition for Database Connection Pooling Service.
 */
@Tags({"cloudera", "hive", "dbcp", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Provides Database Connection Pooling Service for Apache Hive. Connections can be asked from pool and returned after usage.")
public interface Hive3DBCPService extends HiveDBCPService {
}
