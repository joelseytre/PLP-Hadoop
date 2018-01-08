package cs.Lab2.TFIDF;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Round2Map extends Mapper<LongWritable, Text, Text, Text>{

  //input (wordRSJdocid \t wordCount)  (without spaces)
	//output (docid,wordRSJwordCount)


   public void map(LongWritable key, Text value, Context context)
 			throws IOException, InterruptedException{

 		String[] line =  value.toString().split("\t");

    Text wordCount = new Text(line[1]);
 		Text word = new Text(line[0].toString().split("RSJ")[0]);
    Text docid = new Text(line[0].toString().split("RSJ")[1]);

 		Text wordRSJwordCount = new Text(word+"RSJ"+wordCount);
 		context.write(docid, wordRSJwordCount);
 		}
 	}
