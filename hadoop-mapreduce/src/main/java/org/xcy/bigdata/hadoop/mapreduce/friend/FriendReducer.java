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

    IntWritable inputValue = null;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        /*
         * 输入格式：（1表示间接关系，0表示直接关系）
         *
         * 张三-王五 1
         * 张三-王五 1
         * 张三-王五 1
         * 张三-王五 0
         *
         * 或
         *
         * 李四-王五 1
         * 李四-王五 1
         * 李四-王五 1
         * 李四-王五 1
         *
         * 输出格式：
         *
         * 无
         *
         * 或
         *
         * 李四-王五 4
         */

        Iterator<IntWritable> iterator = values.iterator();

        boolean direct = false;
        int sum = 0;
        while (iterator.hasNext()) {
            inputValue = iterator.next();
            // 存在直接关系，立刻终止遍历
            if (inputValue.get() == 0) {
                direct = true;
                break;
            }
            sum++;
        }

        // 没有直接关系，将间接关系累加起来
        if (!direct) {
            context.write(key, new IntWritable(sum));
        }

    }
}
