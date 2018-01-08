package cs.Lab2.PageRank;
import cs.Lab2.PageRank.page_rank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce2 extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
                                                                                InterruptedException {
        
        String links = "";
        double g_PR = 0.0;
        
        for (Text value : values) {
            String in = value.toString();
            
            if (in.startsWith("SEPARATOR")) {

            	String l = content.substring("SEPARATOR".length());
                links += l;
            } else {
                
                double PR = Double.parseDouble(in.split("\t")[0]);
                int TotLinks = Integer.parseInt(in.split("\t")[1]);
                g_PR += (PR / TotLinks);
            }

        }
        
        double n_rank = (page_rank.df)* g_PR + (1-page_rank.df)/(page_rank.NODES.size());
        context.write(key, new Text(n_rank + "\t" + links));
        
    }

}
