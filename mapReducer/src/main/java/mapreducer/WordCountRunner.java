package mapreducer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountRunner {
    public static void main(String[] args) throws Exception{
        Configuration con=new Configuration();
        //创建任务
        Job job=Job.getInstance(con,"wordcount");
        //在集群上运行要加上
        job.setJarByClass(WordCountRunner.class);
        //第一步：设置InputFromat和数据输入的路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPaths(job, String.valueOf(new Path(args[0])));
        //第二步：设置自定义的mapper类并设置输出类型
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //第三第四省略
        //第五步：设置规约
        job.setCombinerClass(WordCountCombiner.class);
        //第六步省略
        //第七步：设置自定义的Reducer类型并设置输出类型
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //设置OutputFormat和数据的输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        //强调，mr默认输出路径是不能存在的，如果存在就保存
        TextOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean b1=job.waitForCompletion(true);
        System.exit(b1?0:1);
    }
}
