/**
 * @Auther icebear
 * @Date 5/13/21
 */

package ETLUtilTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class VideoMain extends Configured implements Tool {
    // 实现run方法
    @Override
    public int run(String[] args) throws Exception{
        // 创建job
        Job job = Job.getInstance(super.getConf(), "washDatas");
        // 为job设置类
        job.setJarByClass(VideoMain.class);
        job.setInputFormatClass(TextInputFormat.class);
        // 设置输入文件路径
        TextInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(VideoMapper.class);
        // 设置输出map的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // 设置输出文件路径
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(4);
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }
    // 实现main方法
    public static void main(String[] args) throws Exception{
        int run = ToolRunner.run(new Configuration(), new VideoMain(), args);
        System.exit(run);
    }
}
