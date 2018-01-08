package cs.Lab2.PageRank;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import cs.Lab2.PageRank.page_rank;

public class Reduce1 extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int c = 0;        
        String l = (1.0/ page_rank.NODES.size()) + "\t";
        for (Text value : values) {
            if (c>0) 
                l += ",";
            l += value.toString();
            c += 1;
        }

        context.write(key, new Text(l));
    }
}
