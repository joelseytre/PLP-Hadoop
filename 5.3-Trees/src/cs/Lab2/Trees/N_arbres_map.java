package cs.Lab2.Trees;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class N_arbres_map extends Mapper <LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] tree =  value.toString().split(";");
		String type = tree[3];
		if (!type.equals("")&& !type.equals("ESPECE") && !type.equals(" ")){
			context.write(new Text(type), new IntWritable(1));
			}
		}
	}
