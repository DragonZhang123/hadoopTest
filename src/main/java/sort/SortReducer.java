package sort;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * Created by zhangguanlong on 2017/11/15.
 */
public class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
    private static IntWritable linenum = new IntWritable(1);

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {

        for (IntWritable val : values) {

            context.write(linenum, key);

            linenum = new IntWritable(linenum.get() + 1);
        }

    }
}
