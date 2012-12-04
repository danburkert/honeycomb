package com.nearinfinity.honeycomb.mysqlengine;

import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Util {
    public static Configuration readConfiguration(File source)
            throws FileNotFoundException {
        Scanner scanner = new Scanner(source);
        Configuration params = new Configuration(false);
        while (scanner.hasNextLine()) {
            Scanner line = new Scanner(scanner.nextLine());
            params.set(line.next(), line.next());
        }
        return params;
    }
}