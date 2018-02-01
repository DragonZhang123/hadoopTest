package flowSum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by zhangguanlong on 2017/11/21.
 */
//job描述和提交的规范写法
public class FlowSumRunner extends Configured implements Tool{
    public int run(String[] strings) throws Exception {
        //
        System.out.println(strings[0]+","+strings[1]);
        Configuration conf =new Configuration();

        Job job =Job.getInstance(conf);

        job.setJarByClass(FlowSumRunner.class);
        job.setMapperClass(FlowSumMapper.class);
        job.setReducerClass(FlowSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job,new Path(strings[0]));
        FileOutputFormat.setOutputPath(job,new Path(strings[1]));

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception{
        int res =ToolRunner.run(new Configuration(),new FlowSumRunner(),args);
        System.out.println(args[0]+","+args[1]);
        System.exit(res);
    }
}
