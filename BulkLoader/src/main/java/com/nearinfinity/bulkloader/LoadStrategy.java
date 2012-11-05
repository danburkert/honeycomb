package com.nearinfinity.bulkloader;

/**
 * Created with IntelliJ IDEA.
 * User: showell
 * Date: 11/2/12
 * Time: 5:14 PM
 * To change this template use File | Settings | File Templates.
 */
public interface LoadStrategy {
    void load() throws Exception;
}
