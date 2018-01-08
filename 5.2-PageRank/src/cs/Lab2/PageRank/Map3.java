package cs.Lab2.PageRank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map3 extends Mapper<LongWritable, Text, DoubleWritable,  Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String l = value.toString().split("\t")[0];
        float score = Float.parseFloat(value.toString().split("\t")[1]);
        float PR=(-1)*score
        
        context.write(new DoubleWritable(PR), new Text(l));
        
    }
       
}
