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
            String[] edge = value.toString().split("\t");
        	String in = edge[0];
        	String out = edge[1];

        	in_node.set(in);
        	out_node.set(out);
        	context.write(in_node, out_node);

        	page_rank.NODES.add(in);
        	page_rank.NODES.add(out);}
            
        
 
    }
    
}