package cs.Lab2.PageRank;


import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class page_rank extends Configured implements Tool{
    

    public static Set<String> NODES = new HashSet<String>();
    public static String LINKS_SEPARATOR = "|";
    

// config
    public static Double df = 0.85;
    public static int IT = 10;
    public static String IN_PAfH = new String();
    public static String OUT_PATH = new String();
    
    

   
//Excecute Run
    public static void main(String[] args) throws Exception {
		      System.out.println(Arrays.toString(args));
		      int res = ToolRunner.run(new Configuration(), new page_rank(), new String [] {"input/", "output/"});
		      System.exit(res);
		   }

//Excecute the 3 mappers/reducers
	public int run(String[] args) throws Exception {
				      
		      System.out.println(Arrays.toString(args));
		      Job job1 = new Job(getConf(), "PR");
		      
		      Path outputFilePath1 = new Path("/home/cloudera/workspace/5.2-PageRank/iterations/iter0");
		      
			    
		    
		      job1.setJarByClass(page_rank.class);
		      job1.setOutputKeyClass(Text.class);
		      job1.setOutputValueClass(Text.class);
		      job1.setMapperClass(Map1.class);
		      job1.setReducerClass(Reduce1.class);
		      job1.setInputFormatClass(TextInputFormat.class);
		      job1.setOutputFormatClass(TextOutputFormat.class);

		      FileInputFormat.addInputPath(job1, new Path(args[0]));
		      FileOutputFormat.setOutputPath(job1, outputFilePath1);
		      
		      job1.waitForCompletion(true);
		      String str = "/home/cloudera/workspace/5.2-PageRank/iterations/iter";
		      for (int i = 0; i < IT; i++) {
		   			String str1 = new String(str + Integer.toString(i+1));
		      		Path outputFilePath2 = new Path(str1);
		      
		      		Job job2 = new Job(getConf(), "PR");
		      		job2.setJarByClass(page_rank.class);
		      		job2.setOutputKeyClass(Text.class);
		      		job2.setOutputValueClass(Text.class);

		      		job2.setMapperClass(Map2.class);
		      		job2.setReducerClass(Reduce2.class);

		      		job2.setInputFormatClass(TextInputFormat.class);
		      		job2.setOutputFormatClass(TextOutputFormat.class);

		      		FileInputFormat.addInputPath(job2, outputFilePath1);
		      		FileOutputFormat.setOutputPath(job2, outputFilePath2);
		     		outputFilePath1 = new Path(str1);
		      		job2.waitForCompletion(true);}
		      
		      
		      Job job3 = new Job(getConf(), "PR");
		      job3.setJarByClass(page_rank.class);
		      job3.setOutputKeyClass(DoubleWritable.class);
		      job3.setOutputValueClass(Text.class);

		      job3.setMapperClass(Map3.class);
		     
		      job3.setInputFormatClass(TextInputFormat.class);
		      job3.setOutputFormatClass(TextOutputFormat.class);

		      FileInputFormat.addInputPath(job3, outputFilePath1);
		      FileOutputFormat.setOutputPath(job3, new Path(args[1]));

		      job3.waitForCompletion(true);

		      
		      return 0;
		    
		   }
}