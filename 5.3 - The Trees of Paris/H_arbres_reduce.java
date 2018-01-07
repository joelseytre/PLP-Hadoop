package cs.Lab2.TreesParis;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class H_arbres_reduce extends Reducer<Text, DoubleWritable, Text,DoubleWritable>{
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException{
		double Height_max = 0; //Maximum height initializiation
		for(DoubleWritable val: values){
			double curr = val.get();
			if ( curr > Height_max)
			{
				Height_max = curr;
			}
		}
		context.write(key, new DoubleWritable(Height_max));
	}
}
