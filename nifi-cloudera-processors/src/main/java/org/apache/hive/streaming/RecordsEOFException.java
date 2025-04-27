package org.apache.hive.streaming;

/**
 * This is a "marker class" used by the HiveRecordWriter to indicate there are no more records in the input stream.
 * It is used by PutHive3Streaming to determine that all records have been written to transaction(s).
 */
public class RecordsEOFException extends SerializationError {

    RecordsEOFException(String msg, Exception e) {
        super(msg, e);
    }
}
