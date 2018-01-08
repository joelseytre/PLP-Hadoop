package cs.Lab2.TFIDF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

public class Round2Reduce extends Reducer<Text, Text, Text, Text>{
  //Input (docid,wordRSJwordCount)
  //Output (wordRSJdocid, wordCountRSJwordsPerDoc)
  private Text wordRSJdocid = new Text();
  private Text wordCountRSJwordsPerDoc = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException{
		int sum = 0;
		List<String> cache = new ArrayList<String>();

		for(Text val: values){
			cache.add(val.toString());
			String[] wordRSJwordCount = val.toString().split("RSJ");
			int wordCount = Integer.parseInt(wordRSJwordCount[1]);
			sum += wordCount;

		}

		for(String val: cache){
			String[] wordRSJwordCount = val.split("RSJ");

      String word = wordRSJwordCount[0];
      String docid = key.toString()
      wordRSJdocid.set(word+"RSJ"+docid);

      String wordCount= wordRSJwordCount[1];
      wordCountRSJwordsPerDoc.set(wordCount+"RSJ"+sum)

			context.write(wordRSJdocid, wordCountRSJwordsPerDoc);

		}
	}
}
