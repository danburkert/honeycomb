package com.nearinfinity.mysqlengine.jni;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Util {
    public static Map<String, String> readParameters(File source)
            throws FileNotFoundException {
        Scanner scanner = new Scanner(source);
        Map<String, String> params = new HashMap<String, String>();
        while (scanner.hasNextLine()) {
            Scanner line = new Scanner(scanner.nextLine());
            params.put(line.next(), line.next());
        }
        return params;
    }
}