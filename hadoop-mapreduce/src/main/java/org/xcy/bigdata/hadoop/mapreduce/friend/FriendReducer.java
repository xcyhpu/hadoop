package org.xcy.bigdata.hadoop.mapreduce.friend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * 描述：
 *
 * @author ：xuchunyang
 * @date ：7/30/21
 */
public class FriendReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        /*
         * 输入格式：
         * 张三-王五 0
         * 张三-王五 0
         * 张三-王五 1
         * 张三-王五 0
         */

        Iterator<IntWritable> iterator = values.iterator();

        int flag = 0;
        while(iterator.hasNext()) {
            iterator.next();
        }

        if(flag > 1) {

        }

    }
}
