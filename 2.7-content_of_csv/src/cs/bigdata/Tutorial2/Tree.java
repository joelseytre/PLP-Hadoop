package cs.bigdata.Tutorial2;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class Tree {
	protected String height;
	protected String year;
	protected String object_id;
	
	
	public static void get_height_year (String line){
		String [] line_split = line.split(";");
		String object_id=line_split[11];
		String height=line_split[6];
		String year=line_split[5];
		System.out.println("Tree number "+object_id+" has a height of "+height+" and is born in "+year);
		
	}
}