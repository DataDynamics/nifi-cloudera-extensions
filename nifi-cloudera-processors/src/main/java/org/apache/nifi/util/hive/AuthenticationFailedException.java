package org.apache.nifi.util.hive;

public class AuthenticationFailedException extends Exception {
    public AuthenticationFailedException(String reason, Exception cause) {
        super(reason, cause);
    }
}