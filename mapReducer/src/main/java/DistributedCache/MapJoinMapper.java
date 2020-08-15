package DistributedCache;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable,Text,Text,Text> {
    //创建一个map集合接收内存中小表的数据
    private static HashMap<String,String> hashMap=new HashMap<String, String>();
    //只执行一次初始化，将内存中数据拿到hashmap中
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        BufferedReader reader=new BufferedReader(new InputStreamReader(new FileInputStream("")));
        String line=null;
        while ((line=reader.readLine())!=null){
            String[] string=line.split(",");
            hashMap.put(string[0],line);
        }
    }
    //处理大表中的数据
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        String s = hashMap.get(split[2]);
        context.write(new Text(split[2]),new Text(s+'\t'+value));
    }
}
