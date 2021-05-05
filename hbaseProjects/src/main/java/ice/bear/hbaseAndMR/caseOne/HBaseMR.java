/**
 * @Auther icebear
 * @Date 5/5/21
 */

package ice.bear.hbaseAndMR.caseOne;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class HBaseMR extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // 创建Job
        Job job = Job.getInstance(super.getConf());
        job.setJarByClass(HBaseMR.class);
        // 设置mapper
        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("myuser"), new Scan(), HBaseReadMapper.class,
                Text.class, Put.class, job);
        // 设置reduce
        TableMapReduceUtil.initTableReducerJob("myuser2", HBaseWriteReducer.class, job);
        // 返回结果
        boolean b = job.waitForCompletion(true);
        return b? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // 创建连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181, node02:2181, node03:2181");
//        Connection connection = ConnectionFactory.createConnection(configuration);

        // 掉用run方法
        int run = ToolRunner.run(configuration, new HBaseMR(), args);
        System.exit(run);

    }
}
