/**
 * @Auther IceBear
 * @Date 5/4/21
 */

package ice.bear.searchCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class SpecialFilterSearch {
    // 连接
    public Connection getConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181, node02:2181, node03:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }
    // 1.单列值过滤器 SingleColumnValueFilter 返回满足条件的cell，所在行的所有cell的值
    // 查询名字为刘备的数据
    public void singleColumnValueFilter() throws IOException{
        // 创建连接
        Connection connection = getConnection();
        // 获取表
        Table table = connection.getTable(TableName.valueOf("myuser"));
        // 创建scan
        Scan scan = new Scan();
        // 单列过滤器，过滤f1列族name列 值为刘备的数据
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("f1".getBytes(),
                "name".getBytes(), CompareFilter.CompareOp.EQUAL, "刘备".getBytes());
        // 设置查询条件
        scan.setFilter(singleColumnValueFilter);
        // 进行查询
        ResultScanner scanner = table.getScanner(scan);
        printlnResult(scanner);
        connection.close();

    }
    // 2.列值排除过滤器SingleColumnValueExcludeFilter
    // 与SingleColumnValueFilter相反，会排除掉指定的列，其他的列全部返回

    // 3.rowkey前缀过滤器PrefixFilter
    // 查询以0001开头的所有前缀的rowkey
    public void prefixFilter() throws IOException{
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf("myuser"));
        Scan scan = new Scan();
        // 过滤rowkey以0001开头的数据
        PrefixFilter prefixFilter = new PrefixFilter("0003".getBytes());
        scan.setFilter(prefixFilter);
        ResultScanner scanner = table.getScanner(scan);
        printlnResult(scanner);
        connection.close();
    }
    // 4.分页过滤器PageFilter
    // 通过pageFilter实现分页过滤器
    public void pageFilter() throws IOException{
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf("myuser"));
        // 页码
        int pageNum = 1;
        // 每页的大小
        int pageSize = 2;
        // scan对象
        Scan scan = new Scan();
        // 获取第一页的数据
        if(pageNum == 1){
            System.out.println("first page!");
            scan.setMaxResultSize(pageSize);
            scan.setStartRow("".getBytes());
            // 使用分页过滤器
            PageFilter pageFilter = new PageFilter(pageSize);
            scan.setFilter(pageFilter);
            ResultScanner scanner = table.getScanner(scan);
            printlnResult(scanner);
        }else{
            System.out.println("not first page!");
            // 如果所读取的分页不是第一页
            // 先取得此分页的第一个rowkey的值
            String startRow = "";
            // 扫描条数
            int scanDatas = (pageNum-1)*pageSize + 1;
            scan.setMaxResultSize(scanDatas);
            // 使用pageFilter
            PageFilter pageFilter = new PageFilter(scanDatas);
            // 设置pageFilter
            scan.setFilter(pageFilter);
            // 获取查询结果
            ResultScanner scanner = table.getScanner(scan);
            // 遍历查询结果，获取第一个rowkry的值
            for(Result result:scanner){
                byte[] row_bytes = result.getRow();
                startRow = Bytes.toString(row_bytes);
            }
            // 设置scan的起始值
            scan.setStartRow(startRow.getBytes());
            // 设置每页的数据条数
            scan.setMaxResultSize(pageSize);
            // 定义pageFilter
            PageFilter pageFilter1 = new PageFilter(pageSize);
            // 设置pageFilter
            scan.setFilter(pageFilter1);
            // 获取查询结果
            ResultScanner scanner1 = table.getScanner(scan);
            // 打印查询结果
            printlnResult(scanner1);
        }
        // 释放资源
        connection.close();
    }
    // 5.多过滤器综合查询FilterList
    // 需求：使用SingleColumnValueFilter查询f1列族，name为孙权的数据，并且同时满足rowkey的前缀以00开头的数据（PrefixFilter）
    public void filterList() throws IOException{
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf("myuser"));
        Scan scan = new Scan();
        // 1.使用 SingleColumnValueFilter
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("f1".getBytes(),
                "name".getBytes(), CompareFilter.CompareOp.EQUAL, "孙权".getBytes());
        // 2.使用 PrefixFilter
        PrefixFilter prefixFilter = new PrefixFilter("000".getBytes());
        // 3.创建 FilterList
        FilterList filterList = new FilterList();
        // 4.将 1,2分别添加到3中
        filterList.addFilter(singleColumnValueFilter);
        filterList.addFilter(prefixFilter);
        // 5.设置查询条件
        scan.setFilter(filterList);
        // 6.获取查询结果
        ResultScanner scanner = table.getScanner(scan);
        // 7.打印查询结果
        printlnResult(scanner);
        // 8.释放资源
        connection.close();
    }



    public void printlnResult(ResultScanner scanner){
        // 遍历查询结果
        for(Result result:scanner){
            List<Cell> cells = result.listCells();
            for(Cell cell:cells){
                byte[] family_name = CellUtil.cloneFamily(cell);
                byte[] qualifier_name = CellUtil.cloneQualifier(cell);
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] value = CellUtil.cloneValue(cell);
                // 判断id和age
                if("age".equals(Bytes.toString(qualifier_name)) || "id".equals(Bytes.toString(qualifier_name))){
                    System.out.println("数据的rowkey为" + Bytes.toString(rowkey) + "==============数据的列族为"
                            + Bytes.toString(family_name) + "===============数据的列名为"
                            + Bytes.toString(qualifier_name) + "================数据的值为" + Bytes.toInt(value));
                }else {
                    System.out.println("数据的rowkey为" + Bytes.toString(rowkey) + "==============数据的列族为"
                            + Bytes.toString(family_name) + "===============数据的列名为"
                            + Bytes.toString(qualifier_name) + "================数据的值为"
                            + Bytes.toString(value));
                }
            }
        }
    }


    public static void main(String[] args) throws IOException {
        SpecialFilterSearch specialFilterSearch = new SpecialFilterSearch();
        // 单列值过滤器
        // specialFilterSearch.singleColumnValueFilter();
        // 前缀过滤器
         specialFilterSearch.prefixFilter();
        // 分页过滤器
        // specialFilterSearch.pageFilter();
        // 组合查询
        // specialFilterSearch.filterList();
    }
}
