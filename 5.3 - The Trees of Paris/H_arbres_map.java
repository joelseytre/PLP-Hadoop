package cs.Lab2.TreesParis;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class heights_map extends Mapper <LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] tree = value.toString().split(";");
			String height = tree[6]; //Get tree "height"

			String type = tree[3];
			if (!type.equals("") && !type.equals(" ") && !type.equals("ESPECE")  && !height.equals("") && !height.equals(" ")){
				context.write(new Text(type), new DoubleWritable(Double.parseDouble(height));
			}
		}
	}
