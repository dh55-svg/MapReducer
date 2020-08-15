package mapreducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words=value.toString().split(" ");
        Text text=new Text();
        LongWritable longWritable=new LongWritable();
        for (String word : words) {
            text.set(word);
            longWritable.set(1);
            context.write(text,longWritable);
        }
    }
}
