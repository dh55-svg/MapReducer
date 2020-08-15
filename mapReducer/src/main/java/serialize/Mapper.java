package serialize;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        Text text=new Text();
        FlowBean flowBean=new FlowBean();
        flowBean.set(Long.parseLong(split[split.length-3]),Long.parseLong(split[split.length-2]));

        text.set(split[0]);
        context.write(text,flowBean);
    }
}
