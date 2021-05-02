package ice.bear;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseVersionAndTTL {
    //private static int sessionTimeout = 4000;
    public static void main(String[] args) throws IOException, InterruptedException {
//       声明配置文件
        Configuration configuration = HBaseConfiguration.create();
//        设置zookeeper
        configuration.set("hbase.zookeeper.quorum", "node01:2181, node02:2181, node03:2181");
//        创建连接
        Connection connection = ConnectionFactory.createConnection(configuration);
//        声明admin
        Admin admin = connection.getAdmin();
//        表不存在则创建
        if(!admin.tableExists(TableName.valueOf("version_hbase"))){
//            声明hbase表
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("version_hbase"));
//            声明列族
            HColumnDescriptor f1 = new HColumnDescriptor("f1");
//            设置下界
            f1.setMinVersions(3);
//            设置上界
            f1.setMaxVersions(5);
//            针对某一列族下的所有列设置TTL
            f1.setTimeToLive(30);
//            将设置好的f1列族添加到hbase表中
            hTableDescriptor.addFamily(f1);
//            利用admin进行表创建
            admin.createTable(hTableDescriptor);
        }
//        向habse表中添加数据
//        声明hbase表
        Table version_table = connection.getTable(TableName.valueOf("version_hbase"));
//        创建rowkey
        Put put = new Put("1".getBytes());
//        针对某一条具体的数据设置TTL
//        put.setTTL(3000);
//        向f1列族中插入数据
        put.addColumn("f1".getBytes(), "name".getBytes(), System.currentTimeMillis(), "zhangsan".getBytes());
//        向hbase表提交数据
        version_table.put(put);
//        休眠一会
        Thread.sleep(1000);
//        设置rowkey
        Put put2 = new Put("1".getBytes());
//        向f1插入第二条数据
        put2.addColumn("f1".getBytes(), "name".getBytes(), System.currentTimeMillis(), "zhangsan2".getBytes());
//        提交数据
        version_table.put(put2);
//        设置f1列族的get方法
        Get get = new Get("1".getBytes());
//        获取结果（列族）
        Result result = version_table.get(get);
//        读取每一列（数组）
        Cell[] cells = result.rawCells();
//        遍历打印输出
        for(Cell cell:cells){
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }
//        关闭hbase表
        version_table.close();
//        关闭连接
        connection.close();

    }
}
