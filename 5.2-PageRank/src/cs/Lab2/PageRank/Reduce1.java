package cs.Lab2.PageRank;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import cs.Lab2.PageRank.page_rank;

public class Reduce1 extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean f = true;        
        String links = (1.0/ page_rank.NODES.size()) + "\t";
        for (Text value : values) {
            if (!f) 
                links += ",";
            links += value.toString();
            f = false;
        }

        context.write(key, new Text(links));
    }
}