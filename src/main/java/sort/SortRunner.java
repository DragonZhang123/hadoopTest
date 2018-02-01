package sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;



import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by zhangguanlong on 2017/11/15.
 */
public class SortRunner {
    public static class Partition extends Partitioner<IntWritable, IntWritable> {

        @Override
        public int getPartition(IntWritable key, IntWritable value,
                                int numPartitions) {
            int MaxNumber = 65223;
            int bound = MaxNumber / numPartitions + 1;
            int keynumber = key.get();
            for (int i = 0; i < numPartitions; i++) {
                if (keynumber < bound * i && keynumber >= bound * (i - 1))
                    return i - 1;
            }
            return 0;
        }
    }
    /**
     * @param args
     */

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Sort");
        job.setJarByClass(SortRunner.class);
        job.setMapperClass(SortMapper.class);
        job.setPartitionerClass(Partition.class);
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/wc/sort1/"));
        FileOutputFormat.setOutputPath(job, new Path("/wc/sort2/"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
