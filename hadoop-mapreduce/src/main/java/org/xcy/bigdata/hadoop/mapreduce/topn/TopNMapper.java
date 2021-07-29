package org.xcy.bigdata.hadoop.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * 描述：
 *
 * @author ：xuchunyang
 * @date ：7/29/21
 */
public class TopNMapper extends Mapper<LongWritable, Text, TopNKey, IntWritable> {

    private TopNKey topNKey = new TopNKey();

    private IntWritable topNValue = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 2021-07-29 11:46:47  110 30
        String[] split = StringUtils.split(value.toString(), '\t');

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(sdf.parse(split[0]));

            topNKey.setYear(calendar.get(Calendar.YEAR));
            topNKey.setMonth(calendar.get(Calendar.MONTH) + 1);
            topNKey.setDay(calendar.get(Calendar.DAY_OF_MONTH));
            topNKey.setTemperator(Integer.valueOf(split[2]));
            topNValue.set(topNKey.getTemperator());
            context.write(topNKey, topNValue);
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}
