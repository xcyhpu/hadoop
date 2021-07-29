import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * 描述：
 *
 * @author ：xuchunyang
 * @date ：7/14/21
 */
public class TestHDFS {

    private static final Logger LOG = LoggerFactory.getLogger(TestHDFS.class);

    Configuration conf = null;

    FileSystem fs = null;

    @Before
    public void init() throws Exception {

//        conf = new Configuration(true);

        conf = new Configuration();

//        fs = FileSystem.get(conf);

    }

    @Test
    public void testMakeDir() throws Exception {

        fs = FileSystem.get(URI.create("hdfs://cluster-hdfs"), conf, "root");

        Path path1 = new Path("/user/root");
        boolean exists = fs.exists(path1);

        LOG.info(path1 + "是否存在：" + exists);

        Path path = new Path("/data/wc/input");
        if(!fs.exists(path)) {
            boolean mkdirs = fs.mkdirs(path);
            System.out.println(mkdirs);
        }
    }

    @Test
    public void testUploadFile() throws Exception {

        // 文件大小 3.72KB
        String src = "/Users/ht_admin/Desktop/HDFS.txt";

        Path dest = new Path("/user/client/HDFS_upload.txt");

        // 这里设置参数不起作用，必须在start-dfs.sh执行前在hdfs-site.xml中设置
//        conf.set("dfs.namenode.fs-limits.min-block-size", "1024");

        // 这种方式可以上传
        fs = FileSystem.get(URI.create("hdfs://cluster-hdfs"), conf, "root");

        // 这种方式上传不了，因为使用本地用户 ht_admin登录到hdfs的，而/user/client是由root用户创建的
        // 除非设置了HADOOP_USER 环境变量
//        fs = FileSystem.get(conf);

        // 3个备份，块大小2048字节（2KB）
        FSDataOutputStream fsDataOutputStream = fs.create(dest, true, 1024, (short) 3, 2048);

//        FSDataOutputStream fsDataOutputStream = fs.create(dest);

        InputStream is = new BufferedInputStream(new FileInputStream(src));

        IOUtils.copyBytes(is, fsDataOutputStream, 1024);

    }

    @After
    public void close() throws Exception {

        fs.close();

    }
}
