package cs.Lab2.TreesParis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class N_arbres_reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int tot = 0; //initializiation
		for(IntWritable val: values){
			tot += val.get(); //add 1 
		}
		context.write(key, new IntWritable(tot));
	}
}
