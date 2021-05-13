/**
 * @Auther icebear
 * @Date 5/13/21
 */

package ETLUtilTest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VideoMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    // 定义两个私有对象
    private Text key2;
    // setup方法
    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
        key2 = new Text();
    }
    // map 方法
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        // 调用washDatas方法对数据进行清理
        String s = VideoUtil.washDatas(value.toString());
        if (null != s){
            key2.set(s);
            context.write(key2, NullWritable.get());
        }
    }

}
