package sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zhangguanlong on 2017/11/15.
 */
public class SortMapper extends Mapper<Object, Text, IntWritable, IntWritable>{

    private static IntWritable data = new IntWritable();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        data.set(Integer.parseInt(line));

        context.write(data, new IntWritable(1));

    }
}
