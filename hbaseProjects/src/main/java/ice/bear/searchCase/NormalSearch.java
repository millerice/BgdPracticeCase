/**
 * @Auther IceBear
 * @Date 5/4/21
 */

package ice.bear.searchCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NormalSearch {
    //创建connection方法
    public Connection getConnection() throws IOException{
        // 设置zookeeper
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181, node02:2181, node03:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }
    // 创建myuser表，此表有两个列族f1和f2
    public void createTable() throws IOException{
        // 创建连接对象
        Connection connection = getConnection();
        // 创建管理员
        Admin admin = connection.getAdmin();
        // 添加表名信息
        HTableDescriptor myuser = new HTableDescriptor(TableName.valueOf("myuser"));
        // 给表添加列族
        myuser.addFamily(new HColumnDescriptor("f1"));
        myuser.addFamily(new HColumnDescriptor("f2"));
        // 创建表
        admin.createTable(myuser);
        // 关闭连接
        admin.close();
        connection.close();
    }
    // 向表中添加数据
    // 初始化连接方法
    public void putData() throws IOException{
        // 创建连接对象
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf("myuser"));
        // 逐条添加数据的方法
        Put put = new Put("0001".getBytes());
        put.addColumn("f1".getBytes(), "name".getBytes(), "zhangdan".getBytes());
        put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(18));
        put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(25));
        put.addColumn("f1".getBytes(), "address".getBytes(), Bytes.toBytes("上海浦东新区"));
        table.put(put);
    }
    // 批量添加数据的方法
    public void batchInsert() throws IOException {
        // 创建连接对象
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf("myuser"));
        //创建put对象，并指定rowkey
        Put put = new Put("0002".getBytes());
        //向f1列族添加数据
        put.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(1));
        put.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("曹操"));
        put.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(30));
        //向f2列族添加数据
        put.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("沛国谯县"));
        put.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("16888888888"));
        put.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("helloworld"));

        Put put2 = new Put("0003".getBytes());
        put2.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(2));
        put2.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("刘备"));
        put2.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(32));
        put2.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put2.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("幽州涿郡涿县"));
        put2.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("17888888888"));
        put2.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("talk is cheap , show me the code"));

        Put put3 = new Put("0004".getBytes());
        put3.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(3));
        put3.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("孙权"));
        put3.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(35));
        put3.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put3.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("下邳"));
        put3.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("12888888888"));
        put3.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("what are you 弄啥嘞！"));

        Put put4 = new Put("0005".getBytes());
        put4.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(4));
        put4.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("诸葛亮"));
        put4.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(28));
        put4.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put4.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("四川隆中"));
        put4.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("14888888888"));
        put4.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("出师表你背了嘛"));

        Put put5 = new Put("0006".getBytes());
        put5.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(5));
        put5.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("司马懿"));
        put5.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(27));
        put5.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put5.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("哪里人有待考究"));
        put5.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("15888888888"));
        put5.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("跟诸葛亮死掐"));


        Put put6 = new Put("0007".getBytes());
        put6.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(5));
        put6.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("xiaobubu—吕布"));
        put6.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(28));
        put6.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put6.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("内蒙人"));
        put6.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("15788888888"));
        put6.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("貂蝉去哪了"));
        List listPut = new ArrayList();
        listPut.add(put);
        listPut.add(put2);
        listPut.add(put3);
        listPut.add(put4);
        listPut.add(put5);
        listPut.add(put6);
        table.put(listPut);
        // 释放资源
        connection.close();
    }
    // get方法查询
    public void getDataByRowkey() throws IOException {
        // 获取连接
        Connection connection = getConnection();
        // 获取表
        Table table = connection.getTable(TableName.valueOf("myuser"));
        // 通过get方法指定rowkey
        Get get = new Get("0003".getBytes());
        // 获取某列族
        get.addFamily("f1".getBytes());
        get.addColumn("f2".getBytes(), "say".getBytes());
        // 将get查询的值封装到一个result对象中
        Result result = table.get(get);
        // 获得result中的所有cell
        List<Cell> cells = result.listCells();
        // 遍历cell，获取值并打印
        for(Cell cell:cells){
            // 获取rowkey
            byte[] rowkey_bytes = CellUtil.cloneRow(cell);
            // 获得列族
            byte[] family_bytes = CellUtil.cloneFamily(cell);
            // 获得列
            byte[] qualifier_bytes = CellUtil.cloneQualifier(cell);
            // 获得值
            byte[] cell_bytes = CellUtil.cloneValue(cell);
            // age 和 id 的值是int类型， 要用Bytes.toInt
            if("age".equals(Bytes.toString(qualifier_bytes)) || "id".equals(Bytes.toString(qualifier_bytes))){
                System.out.println(Bytes.toString(rowkey_bytes));
                System.out.println(Bytes.toString(family_bytes));
                System.out.println(Bytes.toString(qualifier_bytes));
                System.out.println(Bytes.toInt(cell_bytes));
            }else{
                System.out.println(Bytes.toString(rowkey_bytes));
                System.out.println(Bytes.toString(family_bytes));
                System.out.println(Bytes.toString(qualifier_bytes));
                System.out.println(Bytes.toString(cell_bytes));
            }
        }
        connection.close();
    }
    // scan方法查询(批量查询)
    public void scanData() throws IOException{
        // 建立连接
        Connection connection = getConnection();
        // 获取表
        Table table = connection.getTable(TableName.valueOf("myuser"));
        // 创建scan对象
        Scan scan = new Scan();
        // 扫描f1列族
        scan.addFamily("f1".getBytes());
        // 扫描f2列族 phone列
        scan.addColumn("f2".getBytes(), "phone".getBytes());
        // 设置扫描的rowkey范围 前闭后开
        scan.setStartRow("0003".getBytes());
        scan.setStopRow("0007".getBytes());
        // 设置每批次返回的数据条数
        scan.setBatch(20);
        // 设置从cacheBlock中读取数据
        scan.setCacheBlocks(true);
        scan.setMaxResultSize(4);
        scan.setMaxVersions(2);
        // 通过getscanner方法获取数据
        ResultScanner scanner = table.getScanner(scan);
        // 遍历数据并打印
        for(Result result:scanner){
            // result 中封装了每一条数据
            List<Cell> cells = result.listCells();
            for(Cell cell:cells){
                byte[] family_name = CellUtil.cloneFamily(cell);
                byte[] qualifier_name = CellUtil.cloneQualifier(cell);
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] value = CellUtil.cloneValue(cell);
                // 判断id和age字段
                if("age".equals(Bytes.toString(qualifier_name))|| "id".equals(Bytes.toString(qualifier_name))){
                    System.out.println("数据的rowkey为" + Bytes.toString(rowkey) + "==============数据的列族为"
                            + Bytes.toString(family_name) + "===============数据的列名为"
                            + Bytes.toString(qualifier_name) + "================数据的值为" + Bytes.toInt(value));
                }else{
                    System.out.println("数据的rowkey为" + Bytes.toString(rowkey) + "==============数据的列族为"
                            + Bytes.toString(family_name) + "===============数据的列名为"
                            + Bytes.toString(qualifier_name) + "================数据的值为" + Bytes.toString(value));
                }
            }
        }
        // 释放资源
        connection.close();
    }














    public static void main(String[] args) throws IOException {
        NormalSearch normalSearch = new NormalSearch();
        // 创建表
        // normalSearch.createTable();
        // 逐条插入数据
        // normalSearch.putData();
        // 批量插入数据
        // normalSearch.batchInsert();
        // get 方式查询
        // normalSearch.getDataByRowkey();
        // scan 方式查询
        normalSearch.scanData();
    }
}
