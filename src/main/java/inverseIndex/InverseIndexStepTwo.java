package inverseIndex;



import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhang on 2018/2/5.
 */
public class InverseIndexStepTwo {
    public static class StepOneMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] str1 = StringUtils.split(line, "-->");
            ConcurrentHashMap<Object,Object>  FinalKeys = new ConcurrentHashMap<>();




        }
    }
}
