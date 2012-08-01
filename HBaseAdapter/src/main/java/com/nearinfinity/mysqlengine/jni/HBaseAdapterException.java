package com.nearinfinity.mysqlengine.jni;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/1/12
 * Time: 9:53 AM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseAdapterException extends Exception {
    private String message;
    private String stackTraceStr;
    public HBaseAdapterException(String message, String stackTraceStr) {
        this.message = message;
        this.stackTraceStr = stackTraceStr;
    }
    public String getMessage() {
        return this.message;
    }
    public String getStackTraceStr() {
        return this.stackTraceStr;
    }
}
