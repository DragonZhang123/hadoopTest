package flowSum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

/**
 * Created by zhangguanlong on 2017/11/21.
 */
public class FlowSumReducer extends Reducer<Text,FlowBean,Text,FlowBean>{
    //框架每传递一组数据<1387789809,{flowBean,flowBean,flowBean……}>调用
    //遍历values，累加求和在输出
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long up_flow_counter = 0;
        long down_flow_counter = 0;
        for (FlowBean bean:values) {
            up_flow_counter+=bean.getUp_flow();
            down_flow_counter+=bean.getD_flow();
        }
    context.write(key,new FlowBean(key.toString(),up_flow_counter,down_flow_counter));
    }
}
