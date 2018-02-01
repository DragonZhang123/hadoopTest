package areapartation;

import flowSum.FlowBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

/**
 * 对流量原始日志进行流量 统计，将不同省份的用户统计结果输出到不同文件
 * 需要自定义 机制
 * 改造分区逻辑 自定义partitioner
 * 自定义reducer task 并发任务数
 * Created by zhangguanlong on 2017/11/27.
 */
public class FLowSumArea {
    public static class FlowSumAreaMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //拿一行数据
            String line = value.toString();
            //切分成各个字段
            String[] fields = StringUtils.split(line, "\t");
            //拿到字段
            String number = fields[1];
            long u_flow = Long.parseLong(fields[7]);
            long d_flow = Long.parseLong(fields[8]);
            //封装k_v 并输出
            context.write(new Text(number), new FlowBean(number, u_flow, d_flow));
        }
    }

    public static class FlowSumAreaReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long up_flow_count = 0;
            long d_flow_count = 0;
            for (FlowBean bean : values) {
                up_flow_count += bean.getUp_flow();
                d_flow_count += bean.getD_flow();

            }
            context.write(key, new FlowBean(key.toString(), up_flow_count, d_flow_count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(FLowSumArea.class);
        job.setMapperClass(FlowSumAreaMapper.class);
        job.setReducerClass(FlowSumAreaReducer.class);
        //自定义分组逻辑
        job.setPartitionerClass(AreaPartitioner.class);
        //输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //并发
        job.setNumReduceTasks(6);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
