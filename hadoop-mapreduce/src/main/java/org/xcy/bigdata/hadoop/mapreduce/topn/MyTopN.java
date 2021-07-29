package org.xcy.bigdata.hadoop.mapreduce.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 描述：
 *     行记录： 年月日时分秒 城市 气温
 *     求：每个月气温最高的2天
 *
 *     2021-07-28 13:46:57  1   33
 *     2021-05-18 11:46:57  2   37
 *     2021-01-08 11:46:57  1   13
 *     2021-01-08 12:46:57  4   15
 *     2021-01-08 13:46:57  1   19
 *     2021-08-28 13:46:57  5   37
 *
 * @author ：xuchunyang
 * @date ：7/28/21
 */
public class MyTopN {

    private static final Logger LOG = LoggerFactory.getLogger(MyTopN.class);

    public static void main(String[] args) throws Exception {

        LOG.info("开始加载配置信息");

        Configuration conf = new Configuration(true);

        conf.set("mapreduce.framework.name", "local");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        // -D mapreduce.job.reduces=2 /data/topn/input /data/topn/outputArgs
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = parser.getRemainingArgs();

        // 跨平台

        Job job = Job.getInstance(conf);

        // jar包上传到hdfs，在交给yarn执行任务
//        job.setJar("/Users/ht_admin/git/bigdatahadoop/hadoop/hadoop-mapreduce/target/hadoop-mapreduce-1.0.0-SNAPSHOT.jar");
        job.setJarByClass(MyTopN.class);

        // Specify various job-specific parameters
        job.setJobName("TopN_Temperature");

//        Path inputPath = new Path("/data/topn/input");
        Path inputPath = new Path(remainingArgs[0]);
        TextInputFormat.addInputPath(job, inputPath);

//        Path outputPath = new Path("/data/topn/output");
        Path outputPath = new Path(remainingArgs[1]);
        FileSystem fileSystem = outputPath.getFileSystem(conf);

        if(fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        TextOutputFormat.setOutputPath(job, outputPath);


        job.setMapperClass(TopNMapper.class);
        job.setMapOutputKeyClass(TopNKey.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setSortComparatorClass(TopNSortComparator.class);
        job.setPartitionerClass(TopNPartitioner.class);

        job.setGroupingComparatorClass(TopNGroupingComparator.class);
        job.setReducerClass(TopNReducer.class);

        // 可在 args中添加参数 mapreduce.job.reduces=2
//        job.setNumReduceTasks(2);

        // Submit the job, then poll for progress until the job is complete

        // hdfs dfs -chmod -R 777 /tmp
        // 或者设置环境变量 HADOOP_USER_NAME=root
        job.waitForCompletion(true);

    }

}
