package inverseIndex;

import flowSort.SortMR;
import flowSum.FlowBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

/**
 * Created by zhang on 2018/1/23.
 */
public class InverseIndexStepOne {

    public static class StepOneMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            //分出个个单词
            String[] fileds = StringUtils.split(line," ");
            //在切片信息获取文件信息
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String fileName = inputSplit.getPath().getName();
            for (String filed : fileds) {
                //封装kv输出 文件名 -1
                context.write(new Text(filed+"-->"+fileName),new LongWritable(1));
            }

        }
    }
    public static class  StepOneReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        //<hello --> a.txt,{1,1,1}>
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long counter = 0;
            for (LongWritable value : values) {
                counter+=value.get();

            }
            context.write(key,new LongWritable(counter));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf =new Configuration();

        Job job =Job.getInstance(conf);

        job.setJarByClass(InverseIndexStepOne.class);
        job.setMapperClass(StepOneMapper.class);
        job.setReducerClass(StepOneReducer.class);

//        若存在删除输出路径
        Path output = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(output)) {
            fileSystem.delete(output,true);
        }
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }

}
