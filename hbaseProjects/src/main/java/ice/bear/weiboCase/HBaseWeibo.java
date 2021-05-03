package ice.bear.weiboCase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
    // 发布微博内容
    public void publishweibo(String uid, String content) throws IOException{
        /**
         * 微博内容表中添加一条数据，微博收件箱表对所有粉丝用户添加数据
         * 1.将uid微博内容添加到content表
         * 2.从relation表中获得uid的粉丝有哪些fan_uids
         * 3.fan_uids中，每个fan_uid插入数据，uid发送微博时的rowkey email
         */
        // 1.将uid微博内容添加到content表
        Connection connection = getConnection();
        // 获取表
        Table weibo_content = connection.getTable(TableName.valueOf(WEIBO_CONTENT));
        // 创建时间戳
        long timeStamp = System.currentTimeMillis();
        // 创建Rowkey
        String rowkey = uid + "_" + timeStamp;
        // 在指定的Rowkey中添加数据
        Put put = new Put(rowkey.getBytes());
        put.addColumn("info".getBytes(), "content".getBytes(), timeStamp, content.getBytes());
        weibo_content.put(put);
        // 2.从relation表中获得uid的粉丝有哪些fan_uids
        // 获取表
        Table weibo_relation = connection.getTable(TableName.valueOf(WEIBO_RELATION));
        // 利用get方法获取指定用户的粉丝ID
        Get get = new Get(uid.getBytes());
        get.addFamily("fans".getBytes());
        // 如果用户的粉丝不为空，将用户的粉丝ID遍历出来放进一个数组中
        Result result = weibo_relation.get(get);
        // 为空则关闭资源
        if(result.isEmpty()){
            weibo_content.close();
            weibo_relation.close();
            connection.close();
            return;
        }
        Cell[] cells = result.rawCells();
        List<byte[]> fan_uids = new ArrayList<>();
        for(Cell cell:cells){
            // 获取列
            byte[] fan_uid = CellUtil.cloneQualifier(cell);
            fan_uids.add(fan_uid);
        }
        // 3.fan_uids中，每个fan_uid插入数据，uid发送微博时的rowkey email
        // 获取表
        Table weibo_email = connection.getTable(TableName.valueOf(WEIBO_CRCEIVE_CONTENT_EMAIL));
        // 创建一个内容数组
        List<Put> putList = new ArrayList<>();
        // 遍历粉丝的ID，给每一个粉丝添加用户发送微博时的rowkey
        for(byte[] fan_uid:fan_uids){
            Put put1 = new Put(fan_uid);
            put1.addColumn("info".getBytes(), uid.getBytes(), timeStamp, rowkey.getBytes());
            putList.add(put1);
        }
        // 将数组中的内容put到微博邮箱表中
        weibo_email.put(putList);
        // 释放资源
        weibo_content.close();
        weibo_relation.close();
        weibo_email.close();
        connection.close();
    }

    // 添加关注用户
    public void addAttends(String uid, String... attends) throws IOException{
        /**
         * 1.在微博用户关系表中。对当前主动操作的用户添加新关注的好友
         * 2.在微博用户关系表中，对被关注的用户添加新的粉丝
         * 3.微博收件箱表中添加所关注的用户发布的微博
         */
        // A 关注一批用户 B,C ,D
        // 第一步：A是B,C,D的关注者 在weibo:relations 当中attend列族当中以A作为rowkey， B,C,D作为列名，B,C,D作为列值，保存起来
        // 获取连接
        Connection connection = getConnection();
        // 获取表
        Table weibo_relation = connection.getTable(TableName.valueOf(WEIBO_RELATION));
        // 根据uid创建put对象
        Put put = new Put(uid.getBytes());
        // 遍历A用户关注的用户，并结果写入weibo_relation表中
        for(String attend:attends){
            put.addColumn("attends".getBytes(), attend.getBytes(), attend.getBytes());
        }
        weibo_relation.put(put);
        // 第二步：B,C,D都会多一个粉丝A 在weibo:relations 当中fans列族当中分别以B,C,D作为 rowkey，A作为列名，A作为列值，保存起来
        // 遍历A用户关注的人，给关注的人都添加一个A粉丝
        for(String attend:attends){
            // 根据关注人的id创建put对象
            Put put1 = new Put(attend.getBytes());
            // 在fans列族下添加A粉丝
            put1.addColumn("fans".getBytes(), uid.getBytes(), uid.getBytes());
            weibo_relation.put(put1);
        }

        //第三步：去content表查询attends中，每个人发布微博时的rowkey
        // 获取微博内容表
        Table weibo_content = connection.getTable(TableName.valueOf(WEIBO_CONTENT));
        // 创建scan对象
        Scan scan = new Scan();
        // 创建rowkey队列,用户存放A关注的用户的rowkey信息
        ArrayList<byte[]> rowkeyBytes = new ArrayList<>();
        // 遍历A关注的用户ID
        for(String attend:attends) {
            // 对rowkey拼接"_"，后进行扫描
            PrefixFilter prefixFilter = new PrefixFilter((attend + "_").getBytes());
            // 设置查询条件
            scan.setFilter(prefixFilter);
            // 利用scan进行扫描，获取所关注人发送的微博
            ResultScanner scanner = weibo_content.getScanner(scan);
            // 如果当前被关注的人没有发过微博，则跳出循环
            if(null == scanner){
                continue;
            }
            // 否则，遍历扫描到的内容，并添加到rowkey队列中
            for(Result result:scanner){
                byte[] rowkeyWeiboContent = result.getRow();
                rowkeyBytes.add(rowkeyWeiboContent);
            }
        }
        // 第四步：A需要获取B,C,D 的微博内容存放到 receive_content_email 表当中去，以A作为 rowkey，B,C,D作为列名，
        // 获取B,C,D发布的微博rowkey，放到对应的列值里面去
        // 获取表
        Table weibo_email = connection.getTable(TableName.valueOf(WEIBO_CRCEIVE_CONTENT_EMAIL));
        // 判断rowkey队列是否为空
        if(rowkeyBytes.size() > 0) {
            // 不为空，根据A的uid创建put对象
            Put put1 = new Put(uid.getBytes());
            // 遍历rowkey队列
            for(byte[] rowkeyWeiboContent:rowkeyBytes) {
                // 转换成string对象
                String rowkey = Bytes.toString(rowkeyWeiboContent);
                // 对rowkey队列中的rowkey根据"_"进行切分
                String[] split = rowkey.split("_");
                // 在A的put对象中添加拆分后的内容 (列族，列名，版本，内容)
                put1.addColumn("info".getBytes(), split[0].getBytes(), Long.parseLong(split[1]), rowkeyWeiboContent);
            }
            // 在weibo_email中添加put对象
            weibo_email.put(put1);
        }
        // 释放资源
        weibo_content.close();
        weibo_relation.close();
        weibo_email.close();
        connection.close();
    }

    // 获取关注人发送的微博内容
    public void getContent(String uid) throws IOException{
        /**
         * 1.从微博收件箱表中获取所关注人的微博rowkey
         * 2.根据rowkey，得到微博内容
         */
        // 第一步：从 weibo:receive_content_email 表当中获取所有关注人的rowkey
        // 创建连接
        Connection connection = getConnection();
        // 获取表
        Table weibo_email = connection.getTable(TableName.valueOf(WEIBO_CRCEIVE_CONTENT_EMAIL));
        // 根据uid创建get对象
        Get get = new Get(uid.getBytes());
        // 设置获取的最大版本数
        get.setMaxVersions(5);
        // 从weibo_email表中获取A所关注人的id
        Result result = weibo_email.get(get);
        Cell[] cells = result.rawCells();
        // 创建get队列(存放微博的ID)
        ArrayList<Get> gets = new ArrayList<>();
        // 遍历关注人id，并获取关注人发送的微博id(可能存在多个微博信息)
        for(Cell cell:cells){
            byte[] bytes = CellUtil.cloneValue(cell);
            Get get1 = new Get(bytes);
            gets.add(get1);
        }
        // 第二步：通过rowkey（微博id）从weibo:content表当中获取微博内容
        // 获取表
        Table weibo_content = connection.getTable(TableName.valueOf(WEIBO_CONTENT));
        // 根据微博ID获取微博内容
        Result[] results = weibo_content.get(gets);
        // 遍历微博内容，并打印
        for(Result result1:results){
            byte[] weiboContent = result1.getValue("info".getBytes(), "content".getBytes());
            System.out.println(Bytes.toString(weiboContent));
        }
    }

    /**
     * 取关用户：A取消B,C,D这三个用户
     * 1.在微博用户关系表中，对当前主动操作的用户移除取关的好友（attends）
     * 2.在微博用户关系表中，对被取关的用户移除粉丝
     * 3.微博收件箱中删除取关用户发布的微博
     * @param uid, attends
     * @throws IOException
     */
    public void cancelAttends(String uid, String... attends) throws IOException{
        // 第一步：在weibo:relation关系表中，在attends列族当中删除B,C,D这三列
        // 创建连接
        Connection connection = getConnection();
        // 获取表
        Table weibo_relation = connection.getTable(TableName.valueOf(WEIBO_RELATION));
        // 根据uid创建删除对象
        Delete delete = new Delete(uid.getBytes());
        // 遍历要删除的取关用户，添加到删除对象中
        for(String cancelAttend:attends){
            delete.addColumn("attends".getBytes(), cancelAttend.getBytes());
        }
        // 执行表的删除操作
        weibo_relation.delete(delete);
        // 第二步：在weibo:relation关系表中，在fans列族当中，以B,C,D为rowkey，查找fans列族当中A这个粉丝，给删除掉。
        // attends中 A取关的用户，删除A这个粉丝
        for(String cancelAttend:attends){
            Delete delete1 = new Delete(cancelAttend.getBytes());
            delete1.addColumn("fans".getBytes(), uid.getBytes());
            weibo_relation.delete(delete1);
        }
        // 第三步：A取消关注B,C,D,在收件箱中删除取关的人的微博的rowkey
        // 获取表
        Table weibo_email = connection.getTable(TableName.valueOf(WEIBO_CRCEIVE_CONTENT_EMAIL));
        // 创建删除对象
        Delete delete1 = new Delete(uid.getBytes());
        // 遍历attends中的用户ID，添加到删除对象中
        for(String attend:attends){
            delete1.addColumns("info".getBytes(), attend.getBytes());
        }
        // weibo_email中删除A取关用户发布的微博
        weibo_email.delete(delete1);
        // 释放资源
        weibo_relation.close();
        weibo_email.close();
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
        // hBaseWeibo.createTableReceiveContentEmails();
        // 发送微博功能
        // hBaseWeibo.publishweibo("4", "哈士奇简直蠢爆了～");
        // 添加关注用户
//         hBaseWeibo.addAttends("1", "2", "3", "4");
        // 获取关注人发送的微博
         hBaseWeibo.getContent("1");
        // 取关用户
//         hBaseWeibo.cancelAttends("1", "4");
    }
}
