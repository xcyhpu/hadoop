package org.xcy.bigdata.hadoop.mapreduce.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xcy.bigdata.hadoop.mapreduce.topn.*;

/**
 * 描述：
 *
 * @author ：xuchunyang
 * @date ：7/30/21
 */
public class MyFriend {

    private static final Logger LOG = LoggerFactory.getLogger(MyTopN.class);

    public static void main(String[] args) throws Exception {

            LOG.info("开始加载配置信息");

            Configuration conf = new Configuration(true);

            conf.set("mapreduce.framework.name", "local");
            conf.set("mapreduce.app-submission.cross-platform", "true");

            // -D mapreduce.job.reduces=2 /data/friend/input /data/friend/outputArgs
            GenericOptionsParser parser = new GenericOptionsParser(conf, args);
            String[] remainingArgs = parser.getRemainingArgs();

            // 跨平台

            Job job = Job.getInstance(conf);

            // jar包上传到hdfs，在交给yarn执行任务
//        job.setJar("/Users/ht_admin/git/bigdatahadoop/hadoop/hadoop-mapreduce/target/hadoop-mapreduce-1.0.0-SNAPSHOT.jar");
            job.setJarByClass(MyFriend.class);

            // Specify various job-specific parameters
            job.setJobName("Friend");

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

            job.setMapperClass(FriendMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

//            job.setNumReduceTasks(0);

            job.setReducerClass(FriendReducer.class);

            // 可在 args中添加参数 mapreduce.job.reduces=2
//        job.setNumReduceTasks(2);

            // Submit the job, then poll for progress until the job is complete

            // hdfs dfs -chmod -R 777 /tmp
            // 或者设置环境变量 HADOOP_USER_NAME=root
            job.waitForCompletion(true);

    }
}
