package DistributedCache;

import org.apache.commons.httpclient.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MapJoinRunner {
    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,"map_join");

        job.setJarByClass(MapJoinRunner.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPaths(job, String.valueOf(new Path("hdfs://hadoop:9000/hadoop/orders.txt")));
        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("hdfs://hadoop:9000/hadoop/map_join"));
        job.addCacheFile(new java.net.URI("hdfs://hadoop:9000/hadoop/product.txt"));
        boolean s=job.waitForCompletion(true);
        System.exit(s?0:1);
    }
}
