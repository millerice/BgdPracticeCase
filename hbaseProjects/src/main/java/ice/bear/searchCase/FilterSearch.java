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

public class FilterSearch {
    public Connection getConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181, node02:2181, node03:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }
    // 使用rowfilter过滤比rowkey 0003小的所有值
    public void rowFilter() throws IOException{
        // 获取连接
        Connection connection = getConnection();
        // 获取表
        Table table = connection.getTable(TableName.valueOf("myuser"));
        // 创建scan
        Scan scan = new Scan();
        // 获取比较对象
        BinaryComparator binaryComparator = new BinaryComparator("0003".getBytes());
        // 使用rowfilter，RowFilter(比较规则，比较对象)
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS, binaryComparator);
        // 为scan对象设置过滤器
        scan.setFilter(rowFilter);
        // 使用scan查询
        ResultScanner scanner = table.getScanner(scan);
        // 遍历查询结果
        printlnResult(scanner);
    }

    // 列族过滤器FamilyFilter
    // 查询列族名包含f2的所有列族下面的数据
    public void familyFilter() throws IOException{
        // 获取连接
        Connection connection = getConnection();
        // 获取表
        Table table = connection.getTable(TableName.valueOf("myuser"));
        // 创建scan
        Scan scan = new Scan();
        // 获取比较对象
        SubstringComparator substringComparator = new SubstringComparator("f2");
        // 通过familyfilter来设置列族过滤器
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        // 为scan对象设置过滤器
        scan.setFilter(familyFilter);
        // 使用scan查询
        ResultScanner scanner = table.getScanner(scan);
        // 遍历查询结果
        printlnResult(scanner);
    }

    // 列过滤器QualifierFilter
    // 只查询列名包含name的列的值
    public void qualifierFilter() throws IOException {
        // 获取连接
        Connection connection = getConnection();
        // 获取表
        Table table = connection.getTable(TableName.valueOf("myuser"));
        // 创建scan
        Scan scan = new Scan();
        // 获取比较对象
        SubstringComparator substringComparator = new SubstringComparator("name");
        // 定义列名过滤器，只查询列名包含name的列
        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        // 为scan对象设置过滤器
        scan.setFilter(qualifierFilter);
        // 使用scan查询
        ResultScanner scanner = table.getScanner(scan);
        printlnResult(scanner);
    }
    // 列值过滤器ValueFilter
    // 查询所有列当中包含8的数据
    public void contains8() throws IOException{
        // 获取连接
        Connection connection = getConnection();
        // 获取表
        Table table = connection.getTable(TableName.valueOf("myuser"));
        // 创建scan
        Scan scan = new Scan();
        // 获取比较对象
        SubstringComparator substringComparator = new SubstringComparator("8");
        // 列值过滤器，过滤列值中包含8的所有列
        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        // 为scan对象设置过滤器
        scan.setFilter(valueFilter);
        // 使用scan查询
        ResultScanner scanner = table.getScanner(scan);
        printlnResult(scanner);
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
        FilterSearch filterSearch = new FilterSearch();
        // rowKey过滤器RowFilter
        // filterSearch.rowFilter();
        // 列族过滤器FamilyFilter
        // filterSearch.familyFilter();
        // 列过滤器QualifierFilter
        // filterSearch.qualifierFilter();
        // 列值过滤器ValueFilter
        filterSearch.contains8();
    }
}
