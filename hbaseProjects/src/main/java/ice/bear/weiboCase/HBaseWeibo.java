package ice.bear.weiboCase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseWeibo {
    // 微博内容表
    private static final byte[] WEIBO_CONTENT = "weibo:content".getBytes();
    // 用户关系表
    private static final byte[] WEIBO_RELATION = "weibo:relation".getBytes();
    // 收件箱表
    private static final byte[] WEIBO_CRCEIVE_CONTENT_EMAIL = "weibo:receive_content_email".getBytes();
    // 建命名空间
    public void createNameSpace() throws IOException{
        // 获得连接
        Connection connection = getConnection();
        // 生成admin对象
        Admin admin = connection.getAdmin();
        // admin创建namespace
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("weibo").addConfiguration("creator",
                "iceBear").build();
        admin.createNamespace(namespaceDescriptor);
        // 关闭连接
        admin.close();
        connection.close();
    }

    //创建connection方法
    public Connection getConnection() throws IOException{
        // 设置zookeeper
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181, node02:2181, node03:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }
    // 创建表
    public void createTableContent() throws IOException {
        // 获得连接
        Connection connection = getConnection();
        // 获得admin
        Admin admin = connection.getAdmin();
        // 表不存在，创建表并设置版本
        if(!admin.tableExists(TableName.valueOf(WEIBO_CONTENT))) {
            // 设置表名
            HTableDescriptor weibo_content = new HTableDescriptor(TableName.valueOf(WEIBO_CONTENT));
            // 设置列族名
            HColumnDescriptor info = new HColumnDescriptor("info");
            // 指定最大最小版本
            info.setMinVersions(1);
            info.setMaxVersions(1);
            info.setBlockCacheEnabled(true);
            // 表中添加列族
            weibo_content.addFamily(info);
            // admin创建表
            admin.createTable(weibo_content);
        }
        // 关闭连接
        admin.close();
        connection.close();
    }


    // 程序入口
    public static void main(String[] args) throws IOException {
        HBaseWeibo hBaseWeibo = new HBaseWeibo();
        // 创建命名空间
        // hBaseWeibo.createNameSpace();
        // 创建微博内容表
        hBaseWeibo.createTableContent();
    }



}