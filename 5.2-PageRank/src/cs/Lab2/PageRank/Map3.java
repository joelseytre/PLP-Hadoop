package cs.Lab2.PageRank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map3 extends Mapper<LongWritable, Text, DoubleWritable,  Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String info[] = value.toString().split("\t");

        String page = info[0];
        float pageRank = (-1)*Float.parseFloat(info[1]);
        
        context.write(new DoubleWritable(pageRank), new Text(page));
        
    }
       
}