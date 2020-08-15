package serialize;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.IOException;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text,FlowBean,Text,FlowBean> {
    private FlowBean flowBean=new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upFlow=0;
        long downFlow=0;
        for (FlowBean value : values) {
            upFlow+=value.getUpFlow();
            downFlow+=value.getDownFlow();
        }
        flowBean.set(upFlow,downFlow);
        context.write(key,flowBean);
    }
}
