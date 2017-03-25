package BigData.oneStopFlight;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.*;

public class OneStopFlightReducer extends
Reducer<Text, Text, NullWritable, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
		      throws IOException, InterruptedException {
		
//Separate origin and destination values into two separate list
		ArrayList<String> origin = new ArrayList<String>();
		ArrayList<String> dest = new ArrayList<String>();
		
		for (Text val: values){
			if(val.toString().split(",")[4].equals("O")){
				origin.add(val.toString());
			} else {
				dest.add(val.toString());
			}
		}
//Traverse through both lists and write output
		for (String rec1: dest){
			String ftime1 = rec1.split(",")[2];
			for (String rec2: origin){
				String ftime2 = rec2.split(",")[3];
				int depTime = Integer.parseInt(ftime2);
				int arrTime = Integer.parseInt(ftime1);
				//if time difference is greater or equal to 100 and less than r equal to 500 
				//write output
				if((Math.abs(depTime - arrTime) >= 100 && Math.abs(depTime - arrTime) <= 500)){
					String finalVal = key.toString().split(",")[1] + "-" + key.toString().split(",")[2] +
							"-" + key.toString().split(",")[3] + " " + rec1.split(",")[0] + " " +
							rec2.split(",")[0] + " " + rec1.split(",")[1] + " " + key.toString().split(",")[4] + 
							" " +  key.toString().split(",")[4] + " " + rec2.split(",")[1] + " " +
							" " + rec1.split(",")[2] + " " + rec2.split(",")[3];
					context.write(NullWritable.get(), new Text(finalVal));
				}

			}
		}
	}

}
