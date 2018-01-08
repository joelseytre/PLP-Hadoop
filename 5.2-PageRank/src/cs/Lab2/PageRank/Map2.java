package cs.Lab2.PageRank;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs.Lab2.PageRank.page_rank;

import java.io.IOException;

public class Map2 extends Mapper<LongWritable, Text, Text, Text> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        String[] info = value.toString().split("\t");
 
        
        String links = "";
        String page =info[0];
        String pageRank =info[1];
        if (info.length == 3)
        {	links = info[2];
        
//      String[] allPagesLinked = links.split(" ")
        String[] allPagesLinked = links.split(",");
        for (String otherPage : allPagesLinked) { 
            Text pageRankWithTotalLinks = new Text(pageRank + "\t" + Integer.toString(allPagesLinked.length));
            context.write(new Text(otherPage), pageRankWithTotalLinks); 
        }
        } 
        context.write(new Text(page), new Text("SEPARATOR"+ links));
        
    }
    
}