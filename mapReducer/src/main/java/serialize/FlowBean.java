package serialize;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.omg.CORBA.PUBLIC_MEMBER;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {

    private long upFlow;
    private long sumFlow;
    private long downFlow;


    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "upFloe=" + upFlow +
                ", sumFlow=" + sumFlow +
                ", downFlow=" + downFlow +
                '}';
    }



    //必须有一个空参构造方法
    public FlowBean(){
    }

    public void set(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.sumFlow = upFlow+downFlow;
        this.downFlow = downFlow;
    }
    //排序

    //序列化
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }
    //反序列化
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow=dataInput.readLong();
        this.downFlow=dataInput.readLong();
        this.sumFlow=dataInput.readLong();
    }


    public int compareTo(FlowBean o) {
        return (int)(this.sumFlow-o.sumFlow);
    }
}
