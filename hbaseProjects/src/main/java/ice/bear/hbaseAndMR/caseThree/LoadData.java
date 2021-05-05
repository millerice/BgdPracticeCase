/**
 * @Auther icebear
 * @Date 5/5/21
 */

package ice.bear.hbaseAndMR.caseThree;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

import java.io.IOException;

// 加载HFile文件到hbase表中, 不需要打成jar包，上传到服务器执行，本地即可运行
public class LoadData {
    public static void main(String[] args) throws Exception {
        // 建立zk
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181, node02:2181, node03:2181");
        // 获取连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        // 获取admin
        Admin admin = connection.getAdmin();
        // 获取table
        TableName tableName = TableName.valueOf("myuser2");
        Table table = connection.getTable(TableName.valueOf("myuser2"));
        // 构建LoadIncrementalHFiles加载HFile文件
        LoadIncrementalHFiles load = new LoadIncrementalHFiles(configuration);
        // 使用doBulkLoad方法加载数据
        load.doBulkLoad(new Path("hdfs://node01:8020/hbase/out_hfile"), admin, table,
                connection.getRegionLocator(tableName));
        // 释放资源
        connection.close();
    }
}
