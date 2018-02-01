package flowSum;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zhangguanlong on 2017/11/21.
 */
public class FlowSumMapper extends Mapper<LongWritable,Text,Text,FlowBean>{
    //拿到日志中的一行数据，切分各个字段，抽取出需要的字段：number、……然后封装成k—v发送出去
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拿一行数据
        String line =value.toString();
        //切分成各个字段
        String[] fields=StringUtils.split(line,"\t");
        //拿到字段
        String number =fields[1];
        long u_flow =Long.parseLong(fields[7]);
        long d_flow =Long.parseLong(fields[8]);
        //封装k_v
        context.write(new Text(number),new FlowBean(number,  u_flow,  d_flow));

    }
}
