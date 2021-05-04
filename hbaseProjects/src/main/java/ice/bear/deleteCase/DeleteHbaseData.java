/**
 * @Auther IceBear
 * @Date 5/4/21
 */

package ice.bear.deleteCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class DeleteHbaseData {
    // 连接
    public Connection getConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181, node02:2181, node03:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }
    // 根据rowkey删除数据
    // 删除rowkey为003的数据
    public void deleteData() throws IOException{
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf("myuser"));
        Delete delete = new Delete("0003".getBytes());
        // f1列族全部删掉
        delete.addFamily("f1".getBytes());
        // f2列族中删除phone
        delete.addColumn("f2".getBytes(), "phone".getBytes());
        table.delete(delete);
        connection.close();
    }
    // 删除表
    public void deleteTable() throws IOException{
        Connection connection = getConnection();
        // 获取管理员对象
        Admin admin = connection.getAdmin();
        // 禁用表
        admin.disableTable(TableName.valueOf("myuser"));
        // 删除表
        admin.deleteTable(TableName.valueOf("myuser"));
        // 释放资源
        connection.close();
    }


    public static void main(String[] args) throws IOException {
        DeleteHbaseData deleteHbaseData = new DeleteHbaseData();
        // 根据rowkey删除数据
        // deleteHbaseData.deleteData();
        // 删除表
        deleteHbaseData.deleteTable();
    }

}
