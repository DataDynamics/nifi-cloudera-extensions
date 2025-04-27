package org.apache.nifi.util.hive;

import org.apache.hadoop.hive.conf.HiveConf;

import java.io.Serializable;
import java.util.List;


public class HiveOptions implements Serializable {

    protected String databaseName;
    protected String tableName;
    protected String metaStoreURI;
    protected Integer idleTimeout = 60000;
    protected Integer callTimeout = 0;
    protected List<String> staticPartitionValues = null;
    protected String kerberosPrincipal;
    protected String kerberosKeytab;
    protected HiveConf hiveConf;
    protected boolean streamingOptimizations = true;
    protected int transactionBatchSize = 1;

    public HiveOptions(String metaStoreURI, String databaseName, String tableName) {
        this.metaStoreURI = metaStoreURI;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public HiveOptions withCallTimeout(Integer callTimeout) {
        this.callTimeout = callTimeout;
        return this;
    }

    public HiveOptions withStaticPartitionValues(List<String> staticPartitionValues) {
        this.staticPartitionValues = staticPartitionValues;
        return this;
    }

    public HiveOptions withKerberosKeytab(String kerberosKeytab) {
        this.kerberosKeytab = kerberosKeytab;
        return this;
    }

    public HiveOptions withKerberosPrincipal(String kerberosPrincipal) {
        this.kerberosPrincipal = kerberosPrincipal;
        return this;
    }

    public HiveOptions withHiveConf(HiveConf hiveConf) {
        this.hiveConf = hiveConf;
        return this;
    }

    public HiveOptions withStreamingOptimizations(boolean streamingOptimizations) {
        this.streamingOptimizations = streamingOptimizations;
        return this;
    }

    public HiveOptions withTransactionBatchSize(int transactionBatchSize) {
        this.transactionBatchSize = transactionBatchSize;
        return this;
    }

    public String getMetaStoreURI() {
        return metaStoreURI;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getQualifiedTableName() {
        return databaseName + "." + tableName;
    }

    public List<String> getStaticPartitionValues() {
        return staticPartitionValues;
    }

    public Integer getCallTimeOut() {
        return callTimeout;
    }

    public Integer getIdleTimeout() {
        return idleTimeout;
    }

    public HiveConf getHiveConf() {
        return hiveConf;
    }

    public boolean getStreamingOptimizations() {
        return streamingOptimizations;
    }

    public int getTransactionBatchSize() {
        return transactionBatchSize;
    }
}
