package cs.Lab2.TFIDF;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Round1Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	//Output : (wordRSJdocid,wordCount)
	public void reduce(Text wordRSJdocid, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException{
		int sum = 0;
		for(IntWritable val: values){
			sum += val.get();
		}
		context.write(wordRSJdocid, new IntWritable(sum));
	}
}
