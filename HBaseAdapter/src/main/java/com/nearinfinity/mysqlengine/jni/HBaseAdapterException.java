package com.nearinfinity.mysqlengine.jni;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/1/12
 * Time: 9:53 AM
 * To change this template use File | Settings | File Templates.
 */
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
