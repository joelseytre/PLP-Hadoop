package cs.Lab2.TFIDF;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Round3Map extends Mapper<LongWritable, Text, Text, Text>{
  //Input (wordRSJdocid \t wordCountRSJwordsPerDoc) (without spaces)
  //Output (word, docidRSJwordCountRSJwordperdoc)

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{

		String[] line =  value.toString().split("\t");

    String wordRSJdocid  = line[0]
    String word = wordRSJdocid.toString().split("RSJ")[0];
    String docid = wordRSJdocid.toString().split("RSJ")[1];

    String wordCountRSJwordsPerDoc = line[1]
    String wordCount = wordCountRSJwordsPerDoc.toString().split("RSJ")[0];
    String wordsPerDoc = wordCountRSJwordsPerDoc.toString().split("RSJ")[1];

		Text docidRSJcountRSJwordperdoc= new Text(docid+"RSJ"+wordCount+"RSJ"+wordperdoc);
		context.write(new Text(word), docidRSJwordCountRSJwordperdoc);
		}
}
