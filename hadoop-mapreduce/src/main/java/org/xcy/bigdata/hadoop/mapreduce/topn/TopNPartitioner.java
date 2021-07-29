package org.xcy.bigdata.hadoop.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 描述：
 *
 * @author ：xuchunyang
 * @date ：7/29/21
 */
public class TopNPartitioner extends Partitioner<TopNKey, IntWritable> {

    @Override
    public int getPartition(TopNKey topNKey, IntWritable intWritable, int numPartitions) {
        return topNKey.getYear() % numPartitions;
    }
}
