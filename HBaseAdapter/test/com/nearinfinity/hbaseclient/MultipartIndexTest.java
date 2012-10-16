package com.nearinfinity.hbaseclient;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import junit.framework.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class MultipartIndexTest {
    @Test
    public void testMultipartIndexForTable() throws Exception {
        LinkedList<LinkedList<String>> multipart = new LinkedList<LinkedList<String>>();
        multipart.add(new LinkedList<String>() {{
            add("x");
            add("y");
        }});
        Gson gson = new Gson();
        Type type = new TypeToken<LinkedList<LinkedList<String>>>()  {}.getType();
        String json = gson.toJson(multipart, type);
        Map<byte[],byte[]> data = new HashMap<byte[], byte[]>();
        data.put(Constants.MULTIPART_KEY, json.getBytes());
        LinkedList<LinkedList<String>> result = MultipartIndex.indexForTable(data);
        Assert.assertEquals(multipart, result);
    }
}
