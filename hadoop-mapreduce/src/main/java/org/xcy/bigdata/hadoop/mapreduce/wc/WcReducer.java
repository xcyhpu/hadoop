package org.xcy.bigdata.hadoop.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 描述：
 *
 * @author ：xuchunyang
 * @date ：7/21/21
 */
public class WcReducer extends Reducer {

    private IntWritable result = new IntWritable();

    // 相同的key为一组，一组数据调用一次reduce方法
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
