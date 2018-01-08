package cs.Lab2.TFIDF;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class Round1Map extends Mapper<LongWritable, Text, Text, IntWritable>{
	//Output : (wordRSJdocid,1)
	private Text wordRSJdocid = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{

		String docid = ((FileSplit) context.getInputSplit()).getPath().getName();

    for (String word: value.toString().toLowerCase().split("[:;,.?! \'\"()-]")){
			wordRSJdocid.set(word+"RSJ"+docid); //For ReubenSamuelJoel !!
			context.write(wordRSJdocid,  new IntWritable(1));
		}
	}
}
