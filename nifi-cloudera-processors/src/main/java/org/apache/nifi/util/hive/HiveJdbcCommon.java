package org.apache.nifi.util.hive;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.sql.Types.*;

/**
 * JDBC / HiveQL common functions.
 */
public class HiveJdbcCommon {

    public static final String AVRO = "Avro";
    public static final String CSV = "CSV";

    public static final String MIME_TYPE_AVRO_BINARY = "application/avro-binary";
    public static final String CSV_MIME_TYPE = "text/csv";
    public static final PropertyDescriptor NORMALIZE_NAMES_FOR_AVRO = new PropertyDescriptor.Builder()
            .name("hive-normalize-avro")
            .displayName("Normalize Table/Column Names")
            .description("Whether to change non-Avro-compatible characters in column names to Avro-compatible characters. For example, colons and periods "
                    + "will be changed to underscores in order to build a valid Avro record.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();
    private static final Schema TIMESTAMP_MILLIS_SCHEMA = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    private static final Schema DATE_SCHEMA = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    private static final int DEFAULT_PRECISION = 10;
    private static final int DEFAULT_SCALE = 0;

    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream, final int maxRows, boolean convertNames, final boolean useLogicalTypes) throws SQLException, IOException {
        return convertToAvroStream(rs, outStream, null, maxRows, convertNames, null, useLogicalTypes);
    }


    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream, String recordName, final int maxRows, boolean convertNames,
                                           ResultSetRowCallback callback, final boolean useLogicalTypes)
            throws SQLException, IOException {
        final Schema schema = createSchema(rs, recordName, convertNames, useLogicalTypes);
        final GenericRecord rec = new GenericData.Record(schema);

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outStream);

            final ResultSetMetaData meta = rs.getMetaData();
            final int nrOfColumns = meta.getColumnCount();
            long nrOfRows = 0;
            while (rs.next()) {
                if (callback != null) {
                    callback.processRow(rs);
                }
                for (int i = 1; i <= nrOfColumns; i++) {
                    final int javaSqlType = meta.getColumnType(i);
                    Object value = rs.getObject(i);

                    if (value == null) {
                        rec.put(i - 1, null);

                    } else if (javaSqlType == BINARY || javaSqlType == VARBINARY || javaSqlType == LONGVARBINARY || javaSqlType == BLOB || javaSqlType == CLOB) {
                        // bytes requires little bit different handling
                        ByteBuffer bb = null;
                        if (value instanceof byte[]) {
                            bb = ByteBuffer.wrap((byte[]) value);
                        } else if (value instanceof ByteBuffer) {
                            bb = (ByteBuffer) value;
                        }
                        if (bb != null) {
                            rec.put(i - 1, bb);
                        } else {
                            throw new IOException("Could not process binary object of type " + value.getClass().getName());
                        }

                    } else if (value instanceof Byte) {
                        // tinyint(1) type is returned by JDBC driver as java.sql.Types.TINYINT
                        // But value is returned by JDBC as java.lang.Byte
                        // (at least H2 JDBC works this way)
                        // direct put to avro record results:
                        // org.apache.avro.AvroRuntimeException: Unknown datum type java.lang.Byte
                        rec.put(i - 1, ((Byte) value).intValue());

                    } else if (value instanceof BigDecimal) {
                        if (useLogicalTypes) {
                            final int precision = getPrecision(meta.getPrecision(i));
                            final int scale = getScale(meta.getScale(i));
                            rec.put(i - 1, AvroTypeUtil.convertToAvroObject(value, LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES))));
                        } else {
                            rec.put(i - 1, value.toString());
                        }

                    } else if (value instanceof BigInteger) {
                        // Avro can't handle BigDecimal and BigInteger as numbers - it will throw an AvroRuntimeException such as: "Unknown datum type: java.math.BigDecimal: 38"
                        rec.put(i - 1, value.toString());

                    } else if (value instanceof Number) {
                        // Need to call the right getXYZ() method (instead of the getObject() method above), since Doubles are sometimes returned
                        // when the JDBC type is 6 (Float) for example.
                        if (javaSqlType == FLOAT) {
                            value = rs.getFloat(i);
                        } else if (javaSqlType == DOUBLE) {
                            value = rs.getDouble(i);
                        } else if (javaSqlType == INTEGER || javaSqlType == TINYINT || javaSqlType == SMALLINT) {
                            value = rs.getInt(i);
                        }

                        rec.put(i - 1, value);

                    } else if (value instanceof Boolean) {
                        rec.put(i - 1, value);
                    } else if (value instanceof java.sql.SQLXML) {
                        rec.put(i - 1, ((java.sql.SQLXML) value).getString());
                    } else if (useLogicalTypes && javaSqlType == DATE) {
                        rec.put(i - 1, AvroTypeUtil.convertToAvroObject(value, DATE_SCHEMA));
                    } else if (useLogicalTypes && javaSqlType == TIMESTAMP) {
                        rec.put(i - 1, AvroTypeUtil.convertToAvroObject(value, TIMESTAMP_MILLIS_SCHEMA));
                    } else {
                        // The different types that we support are numbers (int, long, double, float),
                        // as well as boolean values, decimal, date, timestamp and Strings. Since Avro doesn't provide
                        // times type, we want to convert those to Strings. So we will cast anything other
                        // than numbers or booleans to strings by using the toString() method.
                        rec.put(i - 1, value.toString());
                    }
                }
                dataFileWriter.append(rec);
                nrOfRows += 1;

                if (maxRows > 0 && nrOfRows == maxRows)
                    break;
            }

            return nrOfRows;
        }
    }

    public static Schema createSchema(final ResultSet rs, boolean convertNames) throws SQLException {
        return createSchema(rs, null, false, false);
    }

    /**
     * Creates an Avro schema from a result set. If the table/record name is known a priori and provided, use that as a
     * fallback for the record name if it cannot be retrieved from the result set, and finally fall back to a default value.
     *
     * @param rs           The result set to convert to Avro
     * @param recordName   The a priori record name to use if it cannot be determined from the result set.
     * @param convertNames Whether to convert column/table names to be legal Avro names
     * @return A Schema object representing the result set converted to an Avro record
     * @throws SQLException if any error occurs during conversion
     */
    public static Schema createSchema(final ResultSet rs, String recordName, boolean convertNames, final boolean useLogicalTypes) throws SQLException {
        final ResultSetMetaData meta = rs.getMetaData();
        final int nrOfColumns = meta.getColumnCount();
        String tableName = StringUtils.isEmpty(recordName) ? "NiFi_SelectHiveQL_Record" : recordName;
        try {
            if (nrOfColumns > 0) {
                // Hive JDBC doesn't support getTableName, instead it returns table.column for column name. Grab the table name from the first column
                String firstColumnNameFromMeta = meta.getColumnName(1);
                int tableNameDelimiter = firstColumnNameFromMeta.lastIndexOf(".");
                if (tableNameDelimiter > -1) {
                    String tableNameFromMeta = firstColumnNameFromMeta.substring(0, tableNameDelimiter);
                    if (!StringUtils.isBlank(tableNameFromMeta)) {
                        tableName = tableNameFromMeta;
                    }
                }
            }
        } catch (SQLException se) {
            // Not all drivers support getTableName, so just use the previously-set default
        }

        if (convertNames) {
            tableName = normalizeNameForAvro(tableName);
        }
        final FieldAssembler<Schema> builder = SchemaBuilder.record(tableName).namespace("any.data").fields();

        /**
         * Some missing Avro types - Decimal, Date types. May need some additional work.
         */
        for (int i = 1; i <= nrOfColumns; i++) {
            String columnNameFromMeta = meta.getColumnName(i);
            // Hive returns table.column for column name. Grab the column name as the string after the last period
            int columnNameDelimiter = columnNameFromMeta.lastIndexOf(".");
            String columnName = columnNameFromMeta.substring(columnNameDelimiter + 1);
            switch (meta.getColumnType(i)) {
                case CHAR:
                case LONGNVARCHAR:
                case LONGVARCHAR:
                case NCHAR:
                case NVARCHAR:
                case VARCHAR:
                case ARRAY:
                case STRUCT:
                case JAVA_OBJECT:
                case OTHER:
                case SQLXML:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                case BIT:
                case BOOLEAN:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().booleanType().endUnion().noDefault();
                    break;

                case INTEGER:
                    // Default to signed type unless otherwise noted. Some JDBC drivers don't implement isSigned()
                    boolean signedType = true;
                    try {
                        signedType = meta.isSigned(i);
                    } catch (SQLException se) {
                        // Use signed types as default
                    }
                    if (signedType) {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().intType().endUnion().noDefault();
                    } else {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().longType().endUnion().noDefault();
                    }
                    break;

                case SMALLINT:
                case TINYINT:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().intType().endUnion().noDefault();
                    break;

                case BIGINT:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().longType().endUnion().noDefault();
                    break;

                // java.sql.RowId is interface, is seems to be database
                // implementation specific, let's convert to String
                case ROWID:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                case FLOAT:
                case REAL:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().floatType().endUnion().noDefault();
                    break;

                case DOUBLE:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().doubleType().endUnion().noDefault();
                    break;

                // Did not find direct suitable type, need to be clarified!!!!
                case DECIMAL:
                case NUMERIC:
                    if (useLogicalTypes) {
                        final int precision = getPrecision(meta.getPrecision(i));
                        final int scale = getScale(meta.getScale(i));
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and()
                                .type(LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES))).endUnion().noDefault();
                    } else {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    }
                    break;

                // Dates were introduced in Hive 0.12.0
                case DATE:
                    if (useLogicalTypes) {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().type(DATE_SCHEMA).endUnion().noDefault();
                    } else {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    }
                    break;
                // Did not find direct suitable type, need to be clarified!!!!
                case TIME:
                case TIMESTAMP:
                    if (useLogicalTypes) {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().type(TIMESTAMP_MILLIS_SCHEMA).endUnion().noDefault();
                    } else {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    }
                    break;

                case BINARY:
                case VARBINARY:
                case LONGVARBINARY:
                case BLOB:
                case CLOB:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().bytesType().endUnion().noDefault();
                    break;


                default:
                    throw new IllegalArgumentException("createSchema: Unknown SQL type " + meta.getColumnType(i) + " cannot be converted to Avro type");
            }
        }

        return builder.endRecord();
    }

    public static long convertToCsvStream(final ResultSet rs, final OutputStream outStream, CsvOutputOptions outputOptions) throws SQLException, IOException {
        return convertToCsvStream(rs, outStream, null, null, outputOptions);
    }

    public static long convertToCsvStream(final ResultSet rs, final OutputStream outStream, String recordName, ResultSetRowCallback callback, CsvOutputOptions outputOptions)
            throws SQLException, IOException {

        final ResultSetMetaData meta = rs.getMetaData();
        final int nrOfColumns = meta.getColumnCount();
        List<String> columnNames = new ArrayList<>(nrOfColumns);

        if (outputOptions.isHeader()) {
            if (outputOptions.getAltHeader() == null) {
                for (int i = 1; i <= nrOfColumns; i++) {
                    String columnNameFromMeta = meta.getColumnName(i);
                    // Hive returns table.column for column name. Grab the column name as the string after the last period
                    int columnNameDelimiter = columnNameFromMeta.lastIndexOf(".");
                    columnNames.add(columnNameFromMeta.substring(columnNameDelimiter + 1));
                }
            } else {
                String[] altHeaderNames = outputOptions.getAltHeader().split(",");
                columnNames = Arrays.asList(altHeaderNames);
            }
        }

        // Write column names as header row
        outStream.write(StringUtils.join(columnNames, outputOptions.getDelimiter()).getBytes(StandardCharsets.UTF_8));
        if (outputOptions.isHeader()) {
            outStream.write("\n".getBytes(StandardCharsets.UTF_8));
        }

        // Iterate over the rows
        int maxRows = outputOptions.getMaxRowsPerFlowFile();
        long nrOfRows = 0;
        while (rs.next()) {
            if (callback != null) {
                callback.processRow(rs);
            }
            List<String> rowValues = new ArrayList<>(nrOfColumns);
            for (int i = 1; i <= nrOfColumns; i++) {
                final int javaSqlType = meta.getColumnType(i);
                final Object value = rs.getObject(i);

                switch (javaSqlType) {
                    case CHAR:
                    case LONGNVARCHAR:
                    case LONGVARCHAR:
                    case NCHAR:
                    case NVARCHAR:
                    case VARCHAR:
                        String valueString = rs.getString(i);
                        if (valueString != null) {
                            // Removed extra quotes as those are a part of the escapeCsv when required.
                            StringBuilder sb = new StringBuilder();
                            if (outputOptions.isQuote()) {
                                sb.append("\"");
                                if (outputOptions.isEscape()) {
                                    sb.append(StringEscapeUtils.escapeCsv(valueString));
                                } else {
                                    sb.append(valueString);
                                }
                                sb.append("\"");
                                rowValues.add(sb.toString());
                            } else {
                                if (outputOptions.isEscape()) {
                                    rowValues.add(StringEscapeUtils.escapeCsv(valueString));
                                } else {
                                    rowValues.add(valueString);
                                }
                            }
                        } else {
                            rowValues.add("");
                        }
                        break;
                    case ARRAY:
                    case STRUCT:
                    case JAVA_OBJECT:
                        String complexValueString = rs.getString(i);
                        if (complexValueString != null) {
                            rowValues.add(StringEscapeUtils.escapeCsv(complexValueString));
                        } else {
                            rowValues.add("");
                        }
                        break;
                    case SQLXML:
                        if (value != null) {
                            rowValues.add(StringEscapeUtils.escapeCsv(((java.sql.SQLXML) value).getString()));
                        } else {
                            rowValues.add("");
                        }
                        break;
                    default:
                        if (value != null) {
                            rowValues.add(value.toString());
                        } else {
                            rowValues.add("");
                        }
                }
            }
            // Write row values
            outStream.write(StringUtils.join(rowValues, outputOptions.getDelimiter()).getBytes(StandardCharsets.UTF_8));
            outStream.write("\n".getBytes(StandardCharsets.UTF_8));
            nrOfRows++;

            if (maxRows > 0 && nrOfRows == maxRows)
                break;
        }
        return nrOfRows;
    }

    public static String normalizeNameForAvro(String inputName) {
        String normalizedName = inputName.replaceAll("[^A-Za-z0-9_]", "_");
        if (Character.isDigit(normalizedName.charAt(0))) {
            normalizedName = "_" + normalizedName;
        }
        return normalizedName;
    }

    public static Configuration getConfigurationFromFiles(final String configFiles) {
        final Configuration hiveConfig = new HiveConf();
        if (StringUtils.isNotBlank(configFiles)) {
            for (final String configFile : configFiles.split(",")) {
                hiveConfig.addResource(new Path(configFile.trim()));
            }
        }
        return hiveConfig;
    }

    //If data in result set contains invalid precision value use Hive default precision.
    private static int getPrecision(int precision) {
        return precision > 1 ? precision : DEFAULT_PRECISION;
    }

    //If data in result set contains invalid scale value use Hive default scale.
    private static int getScale(int scale) {
        return scale > 0 ? scale : DEFAULT_SCALE;
    }

    /**
     * An interface for callback methods which allows processing of a row during the convertToXYZStream() processing.
     * <b>IMPORTANT:</b> This method should only work on the row pointed at by the current ResultSet reference.
     * Advancing the cursor (e.g.) can cause rows to be skipped during Avro transformation.
     */
    public interface ResultSetRowCallback {
        void processRow(ResultSet resultSet) throws IOException;
    }
}
