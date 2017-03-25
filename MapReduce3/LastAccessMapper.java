package BigData.LastAccess;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LastAccessMapper extends Mapper<LongWritable, Text, IPDatePair, Text>{
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		/*
		 * Write intermediate key as IP Address and access time pair. Also pass access time as
		 * value. Input string is split by spaces. The first element is always the IP address. Then
		 * search the rest of the elements to find one that starts with '[', which will form part of the
		 * access time.
		 */
		String[] inputRec = value.toString().split(" ");
		
		String ipAddr = inputRec[0];
		String accessT = "";
		for (int i = 0; i < inputRec.length; i++) {
			if(inputRec[i].matches("^\\[.*?")){
				accessT = inputRec[i] + " " + inputRec[i+1];
				break;
			}
		}
	
		context.write(new IPDatePair(ipAddr, accessT), new Text(accessT));
		
	}

}
