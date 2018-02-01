package areapartation;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * Created by zhangguanlong on 2017/11/27.
 */
public class AreaPartitioner<KEY,VALUE> extends Partitioner<KEY,VALUE>{
    private static HashMap<String,Integer> areaMap =new HashMap<>();
    //连接数据库查询字典表 此处模拟使用
    static {
        areaMap.put("135",0);
        areaMap.put("136",1);
        areaMap.put("137",2);
        areaMap.put("138",3);
        areaMap.put("139",4);

    }
    @Override
    public int getPartition(KEY key, VALUE value, int numPartiations) {
        //从key中拿到手机号，查归属地字典，不同省份返回不同的组号
        //连接jdbc 去MySQL 数据库查询 不可取 因数据量大 考虑数据库承受能力
        //将字典表加载到内存  类初始化的时候加载一次
        int areaCode =areaMap.get(key.toString().substring(0,3))==null?5:areaMap.get(key.toString().substring(0,3));
        return areaCode;
    }
}
