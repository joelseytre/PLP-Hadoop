package cs.Lab2.TFIDF;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SortTFIDFMap extends Mapper<LongWritable, Text, DoubleWritable, Text>{

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{

		for (String line: value.toString().split("\n")){
			String wordRSJdocid = line.split("\t")[0];
			double neg_tfid = Double.parseDouble(line.split("\t")[1]) * (-1);
			context.write(new DoubleWritable(neg_tfid), new Text(word_docid));
		}
	}
}
