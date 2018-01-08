package cs.Lab2.TFIDF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

public class Round3Reduce extends Reducer<Text, Text, Text, DoubleWritable>{
  //Input (word, docidRSJwordCountRSJwordperdoc)
  //Output (wordRSJdocid, tfidf)


	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException{
		int nbDocs = 2;

		int docsPerWord = 0;
    String word =	key.toString()
		Map<String, String> docid_counts = new HashMap<String, String>();
		for(Text docidRSJwordCountRSJwordperdoc: values){

			String docid = docidRSJwordCountRSJwordperdoc.toString().split("RSJ")[0];
      String wordCount = docidRSJwordCountRSJwordperdoc.toString().split("RSJ")[1];
      String wordperdoc = docidRSJwordCountRSJwordperdoc.toString().split("RSJ")[2];
			String wordCountRSJwordperdoc = wordCount +"RSJ"+ wordperdoc;
			docid_counts.put(docid, wordCountRSJwordperdoc);
			docsPerWord += 1;
		}
		for(String docid: docid_counts.keySet()){

			float wordCount = Float.parseFloat(docid_counts.get(docid).split("RSJ")[0]);
			float wordsPerDoc = Float.parseFloat(docid_counts.get(docid).split("RSJ")[1]);

			double tfidf = (wordCount/wordsPerDoc)*Math.log(nbDocs/docsPerWord);
      String wordRSJdocid = word +"RSJ"+ docid
			context.write(new Text(wordRSJdocid), new DoubleWritable(tfidf));
		}
	}
}
