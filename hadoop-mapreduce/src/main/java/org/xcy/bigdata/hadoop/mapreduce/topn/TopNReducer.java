package org.xcy.bigdata.hadoop.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * 描述：
 *
 * @author ：xuchunyang
 * @date ：7/29/21
 */
public class TopNReducer extends Reducer<TopNKey, IntWritable, Text, IntWritable> {


    @Override
    protected void reduce(TopNKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        /**
         * 输出每月温度最高的1天
         */
        Iterator<IntWritable> iterator = values.iterator();

        while (iterator.hasNext()) {
            iterator.next();

            context.write(new Text(key.getYear() + "-" + key.getMonth() + "-" + key.getDay()),
                    new IntWritable(key.getTemperator()));

            break;
        }

//        super.reduce(key, values, context);
    }


}
