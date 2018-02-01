
package flowSort;

import flowSum.FlowBean;
import flowSum.FlowSumMapper;
import flowSum.FlowSumReducer;
import flowSum.FlowSumRunner;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by zhangguanlong on 2017/11/23.
 */
public class SortMR {

    public  static  class  SortMapper extends Mapper<LongWritable,Text, FlowBean,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //拿到一行数据，切分出各个字段，作为一个flowBean输出
            String line =value.toString();
            String[] fields = StringUtils.split(line,"/t");
            String phoneNB = fields[0];
            Long u_flow = Long.parseLong(fields[1]);
            Long d_flow = Long.parseLong(fields[2]);

            context.write(new FlowBean(phoneNB,u_flow,d_flow),NullWritable.get());

        }
    }
    public static class SortReducer extends Reducer<FlowBean,NullWritable,Text,FlowBean>{
        @Override
        protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String phoneNB = key.getNumber();
            context.write(new Text(phoneNB),key);
        }

    }

    public static void main(String[] strings) throws Exception{
        System.out.println(strings[0]+","+strings[1]);
        Configuration conf =new Configuration();

        Job job =Job.getInstance(conf);

        job.setJarByClass(SortMR.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job,new Path(strings[0]));
        FileOutputFormat.setOutputPath(job,new Path(strings[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
