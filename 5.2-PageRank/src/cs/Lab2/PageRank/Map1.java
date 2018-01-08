package cs.Lab2.PageRank;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs.Lab2.PageRank.page_rank;


import java.io.IOException;

public class Map1 extends Mapper<LongWritable, Text, Text, Text> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text in_node = new Text();
        Text out_node = new Text();
        if (value.charAt(0) != '#') {
            String[] in_out = value.toString().split("\t");
        	in_node.set(in_out[0]);
        	out_node.set(in_out[1]);
        	context.write(in_node, out_node);
        	page_rank.NODES.add(in_out[0]);
        	page_rank.NODES.add(in_out[1]);}
            
        
 
    }
    
}
