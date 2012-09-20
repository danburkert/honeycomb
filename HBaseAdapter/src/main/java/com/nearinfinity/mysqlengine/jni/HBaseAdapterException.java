package com.nearinfinity.mysqlengine.jni;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

public class HBaseAdapterException extends Exception {
    private String message;
    private Throwable exception;
    public HBaseAdapterException(String message, Throwable t) {
        this.message = message;
        this.exception = t;
    }
    public String getMessage() {
        return this.message;
    }
    public String getStackTraceStr() {
        if (this.exception == null) {
            return null;
        }
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(outputStream);
        this.exception.printStackTrace(writer);
        writer.close();
        return new String(outputStream.toByteArray());
    }
}
