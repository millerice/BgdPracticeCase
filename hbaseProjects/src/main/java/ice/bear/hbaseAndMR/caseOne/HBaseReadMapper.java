/**
 * @Auther icebear
 * @Date 5/4/21
 */

package ice.bear.hbaseAndMR.caseOne;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

// 需求：读取HBase当中myuser这张表的f1:name、f1:age数据，将数据写入到另外一张myuser2表的f1列族里面去

public class HBaseReadMapper extends TableMapper<Text, Put> {
    // 重写map方法
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
            InterruptedException{
        // 获取rowkey的字节数组
        byte[] rowkey_bytes = key.get();
        String rowkeyStr = Bytes.toString(rowkey_bytes);
        Text text = new Text(rowkeyStr);
        // 构建put对象
        Put put = new Put(rowkey_bytes);
        // 获取value中的cell对象
        Cell[] cells = value.rawCells();
        // 遍历cells，获取name、age
        for(Cell cell:cells){
            // 列族
            byte[] family_bytes = CellUtil.cloneFamily(cell);
            String familyStr = Bytes.toString(family_bytes);
            if ("f1".equals(familyStr)){
                // 判断name和age
                byte[] qualifier_bytes = CellUtil.cloneQualifier(cell);
                String qualifierStr = Bytes.toString(qualifier_bytes);
                if ("name".equals(qualifierStr)){
                    put.add(cell);
                }
                if ("age".equals(qualifierStr)){
                    put.add(cell);
                }
            }
        }
        // 将数据添加到put对象中
        // 判断put对象是否为空
        if (!put.isEmpty()){
            context.write(text, put);
        }
    }

}
