package io.datadynamics.nifi.sql;

import de.siegmar.fastcsv.writer.*;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.Relationship.Builder;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.db.JdbcCommon;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;

@EventDriven
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"sql", "select", "jdbc", "query", "database"})
@WritesAttributes({@WritesAttribute(attribute = "executesql.query.fetch.time", description = "Duration of the result set fetch time in milliseconds. "),
        @WritesAttribute(attribute = "executesql.query.total.time", description = "Duration of the result set fetch time plus the time to process the rows in milliseconds."),
        @WritesAttribute(attribute = "executesql.fetch.count", description = "Contains the number of rows in the outgoing FlowFile. This number will be less than or equal to the value of the Fetch Size property"),
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to 'text/csv'")
})
public class ExecuteFastSQL extends AbstractProcessor {

    private static final String CSV_MIME_TYPE = "text/csv";
    private static final int NUM_THREADS = 10;

    public static final Relationship REL_SUCCESS = (new Builder()).name("success").description("Successfully created FlowFile from SQL query result set.").build();
    public static final Relationship REL_FAILURE = (new Builder()).name("failure").description("SQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship").build();

    public static final PropertyDescriptor DBCP_SERVICE = (new org.apache.nifi.components.PropertyDescriptor.Builder())
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor FETCH_SIZE;
    public static final PropertyDescriptor FETCH_THREADS;
    public static final PropertyDescriptor QUERY_TIMEOUT;
    public static final PropertyDescriptor SQL_SELECT_QUERY;

    private final List<PropertyDescriptor> propDescriptors;

    private final Set<Relationship> relationships;

    protected DBCPService dbcpService;

    public ExecuteFastSQL() {
        List<PropertyDescriptor> pds = new ArrayList();
        pds.add(DBCP_SERVICE);
        pds.add(SQL_SELECT_QUERY);
        pds.add(QUERY_TIMEOUT);
        pds.add(FETCH_SIZE);
        pds.add(FETCH_THREADS);
        this.propDescriptors = Collections.unmodifiableList(pds);
        this.relationships = new HashSet(2);
        this.relationships.add(REL_SUCCESS);
        this.relationships.add(REL_FAILURE);
    }

    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.propDescriptors;
    }

    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile fileToProcess = null;
        if (context.hasIncomingConnection()) {
            fileToProcess = session.get();
            if (fileToProcess == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        ComponentLog logger = this.getLogger();
        int queryTimeout = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions(fileToProcess).asTimePeriod(TimeUnit.SECONDS).intValue();
        Integer fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions(fileToProcess).asInteger();
        Integer fetchThreads = context.getProperty(FETCH_THREADS).evaluateAttributeExpressions(fileToProcess).asInteger();
        String selectQuery;
        if (context.getProperty(SQL_SELECT_QUERY).isSet()) {
            selectQuery = context.getProperty(SQL_SELECT_QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
        } else {
            StringBuilder queryContents = new StringBuilder();
            session.read(fileToProcess, (in) -> {
                queryContents.append(IOUtils.toString(in, Charset.defaultCharset()));
            });
            selectQuery = queryContents.toString();
        }

        this.dbcpService = (DBCPService) context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);

        try {
            Connection con = this.dbcpService.getConnection(fileToProcess == null ? Collections.emptyMap() : fileToProcess.getAttributes());
            Throwable var10 = null;

            try {
                PreparedStatement st = con.prepareStatement(selectQuery);
                Throwable var12 = null;

                try {
                    if (fetchSize != null && fetchSize > 0) {
                        try {
                            st.setFetchSize(fetchSize);
                        } catch (SQLException var45) {
                            logger.debug("Cannot set fetch size to {} due to {}", new Object[]{fetchSize, var45.getLocalizedMessage()}, var45);
                        }
                    }

                    st.setQueryTimeout(queryTimeout);
                    if (fileToProcess != null) {
                        JdbcCommon.setParameters(st, fileToProcess.getAttributes());
                    }

                    logger.debug("Executing query {}", new Object[]{selectQuery});
                    st.execute();
                    ResultSet resultSet = st.getResultSet();
                    int numFetchThreads;
                    if (fetchThreads == null) {
                        logger.warn("The evaluated value of Fetch Threads is not valid, using {}", new Object[]{10});
                        numFetchThreads = 10;
                    } else {
                        numFetchThreads = fetchThreads;
                    }

                    BlockingQueue<Runnable> threadQ = new ArrayBlockingQueue(numFetchThreads);
                    ExecutorService executorService = new ThreadPoolExecutor(numFetchThreads, numFetchThreads, 1L, TimeUnit.SECONDS, threadQ, new CallerRunsPolicy());

                    for (int i = 0; i < fetchThreads; ++i) {
                        executorService.execute(new ExecuteFastSQL.FetchThread(session, resultSet, fetchSize, fileToProcess, context.hasIncomingConnection(), this.getLogger()));
                    }

                    executorService.shutdown();
                    executorService.awaitTermination(1L, TimeUnit.DAYS);
                } catch (Throwable var46) {
                    var12 = var46;
                    throw var46;
                } finally {
                    if (st != null) {
                        if (var12 != null) {
                            try {
                                st.close();
                            } catch (Throwable var44) {
                                var12.addSuppressed(var44);
                            }
                        } else {
                            st.close();
                        }
                    }

                }
            } catch (Throwable var48) {
                var10 = var48;
                throw var48;
            } finally {
                if (con != null) {
                    if (var10 != null) {
                        try {
                            con.close();
                        } catch (Throwable var43) {
                            var10.addSuppressed(var43);
                        }
                    } else {
                        con.close();
                    }
                }

            }
        } catch (SQLException var50) {
            throw new ProcessException(var50);
        } catch (Exception var51) {
            if (fileToProcess != null) {
                session.transfer(fileToProcess, REL_FAILURE);
            }

            if (var51 instanceof ProcessException) {
                throw (ProcessException) var51;
            } else {
                throw new ProcessException(var51);
            }
        }
    }

    static {
        FETCH_SIZE = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("Fetch Size").description("The number of result rows to be fetched from the result set at a time. This is a hint to the database driver and may not be honored and/or exact. If the value specified is zero, then the hint is ignored.").defaultValue("0").required(true).addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();
        FETCH_THREADS = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("Fetch Threads").description("The number of threads to be used to fetched from the result concurrently.").defaultValue(String.valueOf(10)).required(true).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();
        QUERY_TIMEOUT = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("Max Wait Time").description("The maximum amount of time allowed for a running SQL select query  , zero means there is no limit. Max time less than 1 second will be equal to zero.").defaultValue("0 seconds").required(true).addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).sensitive(false).build();
        SQL_SELECT_QUERY = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("SQL select query").description("The SQL select query to execute. The query can be empty, a constant value, or built from attributes using Expression Language. If this property is specified, it will be used regardless of the content of incoming FlowFiles. If this property is empty, the content of the incoming FlowFile is expected to contain a valid SQL select query, to be issued by the processor to the database. Note that Expression Language is not evaluated for FlowFile contents.").required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();
    }

    private static class FetchThread implements Runnable {
        private final ComponentLog logger;
        private final ProcessSession session;
        private final ResultSet resultSet;
        private final int fetchSize;
        private final boolean hasIncomingConnection;
        private final FlowFile inputFlowFile;

        public FetchThread(ProcessSession session, ResultSet rs, Integer fetchSize, FlowFile inputFlowFile, boolean hasIncomingConnection, ComponentLog log) {
            this.session = session;
            this.resultSet = rs;
            this.fetchSize = fetchSize == null ? 1000 : fetchSize;
            this.inputFlowFile = inputFlowFile;
            this.hasIncomingConnection = hasIncomingConnection;
            this.logger = log;
        }

        public void run() {
            StopWatch fetchTime = new StopWatch();
            StopWatch total = new StopWatch();

            while (true) {
                total.start();
                int numberOfRows = 0;

                try {
                    ResultSetMetaData resultSetMetaData = this.resultSet.getMetaData();
                    int columnCount = resultSetMetaData.getColumnCount();
                    FlowFile outputFlowFile = this.inputFlowFile != null ? this.session.create(this.inputFlowFile) : this.session.create();
                    OutputStream flowFileOutputStream = this.session.write(outputFlowFile);
                    Throwable var8 = null;

                    try {
                        CsvWriter csvWriter = CsvWriter.builder().build(new OutputStreamWriter(flowFileOutputStream));
                        Throwable var10 = null;

                        try {
                            fetchTime.start();
                            synchronized (this.resultSet) {
                                while (this.resultSet.next() && numberOfRows < this.fetchSize) {
                                    fetchTime.stop();
                                    String[] rowStrings = new String[columnCount];

                                    for (int i = 0; i < columnCount; ++i) {
                                        Object object = this.resultSet.getObject(i + 1);
                                        rowStrings[i] = object == null ? null : object.toString();
                                    }

                                    csvWriter.writeRecord(rowStrings);
                                    ++numberOfRows;
                                    fetchTime.start();
                                }
                            }

                            fetchTime.stop();
                        } catch (Throwable var42) {
                            var10 = var42;
                            throw var42;
                        } finally {
                            if (csvWriter != null) {
                                if (var10 != null) {
                                    try {
                                        csvWriter.close();
                                    } catch (Throwable var40) {
                                        var10.addSuppressed(var40);
                                    }
                                } else {
                                    csvWriter.close();
                                }
                            }

                        }
                    } catch (Throwable var44) {
                        var8 = var44;
                        throw var44;
                    } finally {
                        if (flowFileOutputStream != null) {
                            if (var8 != null) {
                                try {
                                    flowFileOutputStream.close();
                                } catch (Throwable var39) {
                                    var8.addSuppressed(var39);
                                }
                            } else {
                                flowFileOutputStream.close();
                            }
                        }

                    }

                    total.stop();
                    if (numberOfRows == 0) {
                        this.session.remove(outputFlowFile);
                        return;
                    }

                    if (this.hasIncomingConnection) {
                        this.session.getProvenanceReporter().fetch(outputFlowFile, "Retrieved " + numberOfRows + " rows");
                    } else {
                        this.session.getProvenanceReporter().receive(outputFlowFile, "Retrieved " + numberOfRows + " rows");
                    }

                    Map<String, String> attributes = new HashMap();
                    attributes.put("mime.type", "text/csv");
                    attributes.put("executesql.query.fetch.time", fetchTime.getDuration());
                    attributes.put("executesql.query.total.time", total.getDuration());
                    attributes.put("executesql.fetch.count", String.valueOf(numberOfRows));
                    this.session.putAllAttributes(outputFlowFile, attributes);
                    this.session.transfer(outputFlowFile, ExecuteFastSQL.REL_SUCCESS);
                } catch (SQLException | IOException var46) {
                    throw new RuntimeException(var46);
                }
            }
        }
    }
}
