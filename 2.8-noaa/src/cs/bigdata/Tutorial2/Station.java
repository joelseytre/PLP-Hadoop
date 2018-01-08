package cs.bigdata.Tutorial2;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class Station {
	protected String code;
	protected String name;
	protected String country;
	protected String elevation;
	
	
	protected static void get_station_info (String line){
		String code = line.substring(0, 6);
		String name = line.substring(12,42);
		String country = line.substring(42, 45);
		String elevation = line.substring(73, 81);
		System.out.println("Station " + name + "(code: "+code+") is in country of FIPS -" +country + " at elevation "+elevation);
		
	}
}