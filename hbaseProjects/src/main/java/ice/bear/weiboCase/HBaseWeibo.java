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
    // 创建微博内容表
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

    // 创建用户关系表
    /**
     * 方法名 createTableRelations
     * Table Name  weibo:relations
     * Rowkey  用户 ID
     * ColumnFamily attends fans
     * ColumnLabel 关注用户ID 粉丝用户ID
     * ColumnValue 用户ID
     * Version 1个版本
     * @param
     * @throws IOException
     */
    public void createTableRelation() throws IOException{
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();
        // 表不存在，进行创建
        if(!admin.tableExists(TableName.valueOf(WEIBO_RELATION))){
            // 设置表名
            HTableDescriptor weibo_relation = new HTableDescriptor(TableName.valueOf(WEIBO_RELATION));
            // 设置列族attends
            HColumnDescriptor attends = new HColumnDescriptor("attends");
            // 添加版本
            attends.setMinVersions(1);
            attends.setMaxVersions(1);
            attends.setBlockCacheEnabled(true);
            // 设置列族fans
            HColumnDescriptor fans = new HColumnDescriptor("fans");
            // 添加版本
            fans.setMinVersions(1);
            fans.setMaxVersions(1);
            fans.setBlockCacheEnabled(true);
            // 表中添加列族
            weibo_relation.addFamily(attends);
            weibo_relation.addFamily(fans);
            // 创建表
            admin.createTable(weibo_relation);
        }
        // 关闭
        admin.close();
        connection.close();
    }

    /**
     * 创建微博收件箱表
     * 方法名 createTableReceiveContentEmails
     * Table Name weibo:receive_content_email
     * Rowkey 用户ID
     * ColumnFamily info
     * ColumnLabel 用户ID
     * ColumnValue 取微博内容的Rowkey
     * version 1000
     * @param
     * @throws IOException
     */
    public void createTableReceiveContentEmails() throws IOException{
        // 获得连接
        Connection connection = getConnection();
        // 获得admin
        Admin admin = connection.getAdmin();
        // 创建表
        if(!admin.tableExists(TableName.valueOf(WEIBO_CRCEIVE_CONTENT_EMAIL))){
            HTableDescriptor weibo_receive_content_email =
                    new HTableDescriptor(TableName.valueOf(WEIBO_CRCEIVE_CONTENT_EMAIL));
            HColumnDescriptor info = new HColumnDescriptor("info");
            info.setMinVersions(1000);
            info.setMaxVersions(1000);
            info.setBlockCacheEnabled(true);
            weibo_receive_content_email.addFamily(info);
            admin.createTable(weibo_receive_content_email);
        }
        //关闭连接
        admin.close();
        connection.close();
    }





    // 程序入口
    public static void main(String[] args) throws IOException {
        HBaseWeibo hBaseWeibo = new HBaseWeibo();
        // 创建命名空间
        // hBaseWeibo.createNameSpace();
        // 创建微博内容表
        // hBaseWeibo.createTableContent();
        // 创建用户关系表
        // hBaseWeibo.createTableRelation();
        // 创建微博收件箱表
        hBaseWeibo.createTableReceiveContentEmails();
    }



}
