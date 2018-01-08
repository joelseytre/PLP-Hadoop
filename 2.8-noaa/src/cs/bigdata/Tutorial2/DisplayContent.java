package cs.bigdata.Tutorial2;


import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class DisplayContent {

	public static void main(String[] args) throws IOException {
		
		String localSrc = "/home/cloudera/workspace/2.8-noaa/isd-history.txt";
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			
			int t = 0;
			while (line !=null){
				t++;
				if(t<=22){
					// go to the next line
					line = br.readLine();
				}
				else{
					// print info
					Station.get_station_info(line);
					// go to the next line
					line = br.readLine();
				}
			}
		}
		finally{
			//close the file
			in.close();
			fs.close();
		}
 
		
		
	}

}
