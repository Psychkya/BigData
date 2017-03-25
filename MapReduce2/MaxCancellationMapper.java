package BigData.MaxCancellation;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;

public class MaxCancellationMapper extends
Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException {
		//Splitting the comma separated values to an array
			String[] csvValue = value.toString().split(",");
			String val = "";
		//Check that the starting value is numeric - this will eliminate the header record
		//Key will be unique airline and value will the orig/dest pair for those that were cancelled
			if (csvValue[0].matches("\\d+") && csvValue[21].equals("1")){
					val = csvValue[16] + "," + csvValue[17];
					context.write(new Text(csvValue[8]), new Text(val));
				}

	}

}