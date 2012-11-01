package com.nearinfinity.datacreator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class DiasporaDataMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Random random = new Random();
    private static final Date startDate = new GregorianCalendar(2010, 1, 1).getTime();
    private static final Date endDate = new GregorianCalendar(2012, 10, 30).getTime();
    private static final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Map<Integer, String> users = new HashMap<Integer, String>();
    private int numRows = 0;
    private long seed = 0;

    private static final String[] messages = {
            "Hello, World!",
            "I can't believe how incredibly slow the bus system is. I waited 5 hours....bah",
            "Lol can't stop laughing at this show #hilarious",
            "liek dis if u cry ever tim",
            "Has Diaspora changed their layout AGAIN?!?!?!",
            "Hey everyone, party at my place tonight for my birthday. Come out and party!",
            "#YOLO",
            "I've found that I can cut 20 minutes off my commute!",
            "Check out this awesome link http://bit.ly/1a09p",
            "Gotta work a double shift....not fun",
            "HBase is the greatest thing since sliced bread!",
            "Go Redskins! #HTTR",
            "Just got into grad school at VT! Awesome!",
            "Waiting for code to compile...so bored..."
    };

    public static Date generateDateInRange(Date start, Date end) {
        long startTime = start.getTime();
        long endTime = end.getTime();
        long date = startTime + (long) (Math.random() * (endTime - startTime));
        return new Date(date);
    }

    private String createDataRow(long id) {
        List<String> list = new LinkedList<String>();
        int userId = Math.min((int) (Math.random() * users.size()), 1);
        int messageId = (int) (Math.random() * messages.length);
        Date createdAt = generateDateInRange(startDate, endDate);

        list.add("" + id);                                            /* id */
        list.add("" + userId);                                        /* author_id */
        list.add("0");                                              /* public */
        list.add(users.get(userId));                                /* diaspora_handle */
        list.add(Long.toHexString(random.nextLong()));              /* guid */
        list.add("0");                                              /* pending */
        list.add("StatusMessage");                                  /* type */
        list.add("\"" + messages[messageId] + "\"");                /* text */
        list.add("");                                               /* remote_photo_path */
        list.add("");                                               /* remote_photo_name */
        list.add("");                                               /* random_string */
        list.add("");                                               /* processed_image */
        list.add(formatDate(createdAt));                            /* created_at */
        list.add(formatDate(createdAt));                            /* updated_at */
        list.add("");                                               /* unprocessed_image */
        list.add("");                                               /* object_url */
        list.add("");                                               /* image_url */
        list.add("");                                               /* image_height */
        list.add("");                                               /* image_width */
        list.add("");                                               /* provider_display_name */
        list.add("");                                               /* actor_url */
        list.add("");                                               /* objectId */
        list.add("");                                               /* root_guid */
        list.add("");                                               /* status_message_guid */
        list.add("0");                                              /* likes_count */
        list.add("0");                                              /* comments_count */
        list.add("");                                               /* o_embed_cache_id */
        list.add("0");                                              /* reshares_count */
        list.add(formatDate(createdAt));                            /* interacted_at */
        list.add("");                                               /* frame_name */
        list.add("0");                                              /* favorite */

        return StringUtils.join(list, ",");
    }

    private String formatDate(Date date) {
        return DF.format(date);
    }

    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();

        //Extract the number of rows
        numRows = Integer.parseInt(conf.get("numRows"));

        //Extract the users
        String userString = conf.get("users");
        for (String user : userString.split(",")) {
            String[] tokens = user.split(";");
            users.put(Integer.parseInt(tokens[0]), tokens[1]);
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //There will only be one line in the file. This will contain the file number.
        //We can set the uniqueness seed from this number.
        int fileNumber = Integer.parseInt(value.toString());
        seed = (long) fileNumber * (long) numRows;

        //Now, create our data
        String row = "";
        for (long i = seed; i < seed + numRows; i++) {
            row = createDataRow(i);
            context.write(new Text(row), null);
        }
    }
}
