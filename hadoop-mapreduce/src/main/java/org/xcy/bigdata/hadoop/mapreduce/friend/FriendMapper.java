package org.xcy.bigdata.hadoop.mapreduce.friend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * 描述：
 *
 * @author ：xuchunyang
 * @date ：7/30/21
 */
public class FriendMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outputKey = new Text();
    private IntWritable outputValue = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 张三 王五 李四 赵四
        // 张三后面三个人是张三的朋友（直接关系0），后面三个人之间是间接关系1
        /*
         * 当前记录输出为：
         * 张三-王五	0
         * 张三-李四	0
         * 张三-赵四	0
         * 王五-李四	1
         * 王五-赵四	1
         * 李四-赵四	1
         */
        String[] split = StringUtils.split(value.toString(), ' ');
        for (int i = 0; i < split.length; i++) {
            for (int j = i + 1; j < split.length; j++) {
                outputKey.set(buildRelation(split[i], split[j]));
                outputValue.set(i == 0 ? 0 : 1);
                context.write(outputKey, outputValue);
            }
        }
    }

    private String buildRelation(String s1, String s2) {

        return s1.compareTo(s2) < 0 ? s1 + "-" + s2 : s2 + "-" + s1;
    }
}
