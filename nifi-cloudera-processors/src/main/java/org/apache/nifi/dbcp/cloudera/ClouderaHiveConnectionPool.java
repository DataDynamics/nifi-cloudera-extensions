package org.apache.nifi.dbcp.cloudera;

import com.cloudera.hive.jdbc.HS2Driver;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.dbcp.DBCPValidator;
import org.apache.nifi.dbcp.hive.ClouderaHiveDBCPService;
import org.apache.nifi.processors.hive.HiveConfigurator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.krb.KerberosLoginException;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.util.hive.AuthenticationFailedException;
import org.apache.nifi.util.hive.ValidationResources;

import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@RequiresInstanceClassLoading
@Tags({"hive", "dbcp", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Provides Database Connection Pooling Service for Apache Hive 3.x. Connections can be asked from pool and returned after usage.")
public class ClouderaHiveConnectionPool extends AbstractControllerService implements ClouderaHiveDBCPService {
    private static final String DEFAULT_MIN_IDLE = "0";

    private static final String DEFAULT_MAX_IDLE = "8";

    private static final String DEFAULT_MAX_CONN_LIFETIME = "-1";

    private static final String DEFAULT_EVICTION_RUN_PERIOD = String.valueOf(-1L);

    private static final String DEFAULT_MIN_EVICTABLE_IDLE_TIME = "30 mins";

    private static final String DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME = String.valueOf(-1L);

    static final PropertyDescriptor DATABASE_URL = (new PropertyDescriptor.Builder())
            .name("hive-db-connect-url")
            .displayName("Database Connection URL")
            .description("A database connection URL used to connect to a database. May contain database system name, host, port, database name and some parameters. The exact syntax of a database connection URL is specified by the Hive documentation. For example, the server principal is often included as a connection parameter when connecting to a secure Hive server.")

            .defaultValue(null)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor HIVE_CONFIGURATION_RESOURCES = (new PropertyDescriptor.Builder())
            .name("hive-config-resources")
            .displayName("Hive Configuration Resources")
            .description("A file or comma separated list of files which contains the Hive configuration (hive-site.xml, e.g.). Without this, Hadoop will search the classpath for a 'hive-site.xml' file or will revert to a default configuration. Note that to enable authentication with Kerberos e.g., the appropriate properties must be set in the configuration files. Please see the Hive documentation for more details.")

            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, new ResourceType[0])
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor DB_USER = (new PropertyDescriptor.Builder())
            .name("hive-db-user")
            .displayName("Database User")
            .description("Database user name")
            .defaultValue(null)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor DB_PASSWORD = (new PropertyDescriptor.Builder())
            .name("hive-db-password")
            .displayName("Password")
            .description("The password for the database user")
            .defaultValue(null)
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor MAX_WAIT_TIME = (new PropertyDescriptor.Builder())
            .name("hive-max-wait-time")
            .displayName("Max Wait Time")
            .description("The maximum amount of time that the pool will wait (when there are no available connections)  for a connection to be returned before failing, or -1 to wait indefinitely. ")

            .defaultValue("500 millis")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor MAX_TOTAL_CONNECTIONS = (new PropertyDescriptor.Builder())
            .name("hive-max-total-connections")
            .displayName("Max Total Connections")
            .description("The maximum number of active connections that can be allocated from this pool at the same time, or negative for no limit.")

            .defaultValue("8")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor VALIDATION_QUERY = (new PropertyDescriptor.Builder())
            .name("Validation-query")
            .displayName("Validation query")
            .description("Validation query used to validate connections before returning them. When a borrowed connection is invalid, it gets dropped and a new valid connection will be returned. NOTE: Using validation may have a performance penalty.")

            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MIN_IDLE = (new PropertyDescriptor.Builder())
            .displayName("Minimum Idle Connections")
            .name("dbcp-min-idle-conns")
            .description("The minimum number of connections that can remain idle in the pool, without extra ones being created, or zero to create none.")

            .defaultValue("0")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_IDLE = (new PropertyDescriptor.Builder())
            .displayName("Max Idle Connections")
            .name("dbcp-max-idle-conns")
            .description("The maximum number of connections that can remain idle in the pool, without extra ones being released, or negative for no limit.")

            .defaultValue("8")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_CONN_LIFETIME = (new PropertyDescriptor.Builder())
            .displayName("Max Connection Lifetime")
            .name("dbcp-max-conn-lifetime")
            .description("The maximum lifetime in milliseconds of a connection. After this time is exceeded the connection will fail the next activation, passivation or validation test. A value of zero or less means the connection has an infinite lifetime.")

            .defaultValue("-1")
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor EVICTION_RUN_PERIOD = (new PropertyDescriptor.Builder())
            .displayName("Time Between Eviction Runs")
            .name("dbcp-time-between-eviction-runs")
            .description("The number of milliseconds to sleep between runs of the idle connection evictor thread. When non-positive, no idle connection evictor thread will be run.")

            .defaultValue(DEFAULT_EVICTION_RUN_PERIOD)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MIN_EVICTABLE_IDLE_TIME = (new PropertyDescriptor.Builder())
            .displayName("Minimum Evictable Idle Time")
            .name("dbcp-min-evictable-idle-time")
            .description("The minimum amount of time a connection may sit idle in the pool before it is eligible for eviction.")
            .defaultValue("30 mins")
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor SOFT_MIN_EVICTABLE_IDLE_TIME = (new PropertyDescriptor.Builder())
            .displayName("Soft Minimum Evictable Idle Time")
            .name("dbcp-soft-min-evictable-idle-time")
            .description("The minimum amount of time a connection may sit idle in the pool before it is eligible for eviction by the idle connection evictor, with the extra condition that at least a minimum number of idle connections remain in the pool. When the not-soft version of this option is set to a positive value, it is examined first by the idle connection evictor: when idle connections are visited by the evictor, idle time is first compared against it (without considering the number of idle connections in the pool) and then against this soft option, including the minimum idle connections constraint.")

            .defaultValue(DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static final PropertyDescriptor KERBEROS_USER_SERVICE = (new PropertyDescriptor.Builder())
            .name("kerberos-user-service")
            .displayName("Kerberos User Service")
            .description("Specifies the Kerberos User Controller Service that should be used for authenticating with Kerberos")
            .identifiesControllerService(KerberosUserService.class)
            .required(false)
            .build();

    private List<PropertyDescriptor> properties;

    private String connectionUrl = "unknown";

    private final AtomicReference<ValidationResources> validationResourceHolder = new AtomicReference<>();

    private volatile BasicDataSource dataSource;

    private final HiveConfigurator hiveConfigurator = new HiveConfigurator();

    private volatile UserGroupInformation ugi;

    private final AtomicReference<KerberosUser> kerberosUserReference = new AtomicReference<>();

    protected void init(ControllerServiceInitializationContext context) {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(DATABASE_URL);
        props.add(HIVE_CONFIGURATION_RESOURCES);
        props.add(DB_USER);
        props.add(DB_PASSWORD);
        props.add(KERBEROS_USER_SERVICE);
        props.add(MAX_WAIT_TIME);
        props.add(MAX_TOTAL_CONNECTIONS);
        props.add(VALIDATION_QUERY);
        props.add(MIN_IDLE);
        props.add(MAX_IDLE);
        props.add(MAX_CONN_LIFETIME);
        props.add(EVICTION_RUN_PERIOD);
        props.add(MIN_EVICTABLE_IDLE_TIME);
        props.add(SOFT_MIN_EVICTABLE_IDLE_TIME);
        this.properties = props;
    }

    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.properties;
    }

    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        boolean confFileProvided = validationContext.getProperty(HIVE_CONFIGURATION_RESOURCES).isSet();
        List<ValidationResult> problems = new ArrayList<>();
        if (confFileProvided) {
            KerberosUserService kerberosUserService = (KerberosUserService) validationContext.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
            String configFiles = validationContext.getProperty(HIVE_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
            problems.addAll(this.hiveConfigurator.validate(configFiles, kerberosUserService, this.validationResourceHolder, getLogger()));
        }
        return problems;
    }

    @OnEnabled
    public void onConfigured(ConfigurationContext context) throws InitializationException {
        ComponentLog log = getLogger();
        String configFiles = context.getProperty(HIVE_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
        HiveConf hiveConf = this.hiveConfigurator.getConfigurationFromFiles(configFiles);
        String validationQuery = context.getProperty(VALIDATION_QUERY).evaluateAttributeExpressions().getValue();
        for (Map.Entry<PropertyDescriptor, String> entry : (Iterable<Map.Entry<PropertyDescriptor, String>>) context.getProperties().entrySet()) {
            PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic())
                hiveConf.set(descriptor.getName(), context.getProperty(descriptor).evaluateAttributeExpressions().getValue());
        }
        String drv = HS2Driver.class.getName();
        if (SecurityUtil.isSecurityEnabled((Configuration) hiveConf)) {
            KerberosUserService kerberosUserService = (KerberosUserService) context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
            if (kerberosUserService == null)
                throw new InitializationException("Kerberos authentication is required by the specified configuration, Kerberos User Service must be configured");
            KerberosUser kerberosUser = kerberosUserService.createKerberosUser();
            try {
                this.ugi = this.hiveConfigurator.authenticate((Configuration) hiveConf, kerberosUser);
            } catch (AuthenticationFailedException ae) {
                log.error(ae.getMessage(), (Throwable) ae);
                throw new InitializationException(ae);
            }
            this.kerberosUserReference.set(kerberosUser);
            getLogger().info("Successfully logged in as principal " + kerberosUser.getPrincipal());
        }
        String user = context.getProperty(DB_USER).evaluateAttributeExpressions().getValue();
        String passw = context.getProperty(DB_PASSWORD).evaluateAttributeExpressions().getValue();
        Long maxWaitMillis = context.getProperty(MAX_WAIT_TIME).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        Integer maxTotal = context.getProperty(MAX_TOTAL_CONNECTIONS).evaluateAttributeExpressions().asInteger();
        Integer minIdle = context.getProperty(MIN_IDLE).evaluateAttributeExpressions().asInteger();
        Integer maxIdle = context.getProperty(MAX_IDLE).evaluateAttributeExpressions().asInteger();
        Long maxConnLifetimeMillis = extractMillisWithInfinite(context.getProperty(MAX_CONN_LIFETIME).evaluateAttributeExpressions());
        Long timeBetweenEvictionRunsMillis = extractMillisWithInfinite(context.getProperty(EVICTION_RUN_PERIOD).evaluateAttributeExpressions());
        Long minEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(MIN_EVICTABLE_IDLE_TIME).evaluateAttributeExpressions());
        Long softMinEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(SOFT_MIN_EVICTABLE_IDLE_TIME).evaluateAttributeExpressions());
        this.dataSource = new BasicDataSource();
        this.dataSource.setDriverClassName(drv);
        this.connectionUrl = context.getProperty(DATABASE_URL).evaluateAttributeExpressions().getValue();
        if (validationQuery != null && !validationQuery.isEmpty()) {
            this.dataSource.setValidationQuery(validationQuery);
            this.dataSource.setTestOnBorrow(true);
        }
        this.dataSource.setUrl(this.connectionUrl);
        this.dataSource.setUsername(user);
        this.dataSource.setPassword(passw);
        this.dataSource.setMaxWaitMillis(maxWaitMillis.longValue());
        this.dataSource.setMaxTotal(maxTotal.intValue());
        this.dataSource.setMinIdle(minIdle.intValue());
        this.dataSource.setMaxIdle(maxIdle.intValue());
        this.dataSource.setMaxConnLifetimeMillis(maxConnLifetimeMillis.longValue());
        this.dataSource.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis.longValue());
        this.dataSource.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis.longValue());
        this.dataSource.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis.longValue());
    }

    private Long extractMillisWithInfinite(PropertyValue prop) {
        return Long.valueOf("-1".equals(prop.getValue()) ? -1L : prop.asTimePeriod(TimeUnit.MILLISECONDS).longValue());
    }

    @OnDisabled
    public void shutdown() {
        try {
            this.dataSource.close();
        } catch (SQLException e) {
            throw new ProcessException(e);
        }
    }

    public Connection getConnection() throws ProcessException {
        try {
            if (this.ugi != null) {
                getLogger().trace("getting UGI instance");
                if (this.kerberosUserReference.get() != null) {
                    KerberosUser kerberosUser = this.kerberosUserReference.get();
                    getLogger().debug("kerberosUser is " + kerberosUser);
                    try {
                        getLogger().debug("checking TGT on kerberosUser " + kerberosUser);
                        kerberosUser.checkTGTAndRelogin();
                    } catch (KerberosLoginException e) {
                        throw new ProcessException("Unable to relogin with kerberos credentials for " + kerberosUser.getPrincipal(), e);
                    }
                } else {
                    getLogger().debug("kerberosUser was null, will not refresh TGT with KerberosUser");
                    this.ugi.checkTGTAndReloginFromKeytab();
                }
                try {
                    return ugi.doAs((PrivilegedExceptionAction<Connection>) () -> dataSource.getConnection());
                } catch (UndeclaredThrowableException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof SQLException)
                        throw (SQLException) cause;
                    throw e;
                }
            }
            getLogger().info("Simple Authentication");
            return this.dataSource.getConnection();
        } catch (SQLException | java.io.IOException | InterruptedException e) {
            getLogger().error("Error getting Hive connection", e);
            throw new ProcessException(e);
        }
    }

    public String toString() {
        return "ClouderaHiveConnectionPool[id=" + getIdentifier() + "]";
    }

    public String getConnectionURL() {
        return this.connectionUrl;
    }
}
