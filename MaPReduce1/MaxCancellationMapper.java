package BigData.MaxCancellation;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;

public class MaxCancellationMapper extends
Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException {
		//Splitting the comma separated values to an array
			String[] csvValue = value.toString().split(",");
		//Check that the starting value is numeric - this will elimiate the header record
			if (csvValue[0].matches("\\d+")){
		//Creating key with unique airline and origin & destination pair
				String keyVal = csvValue[8] + "\t" + csvValue[16]+ "," + csvValue[17];
				int val = 0;
		//Cancellation field may contain non-numeric such as 'NA', so checking for numeric
		//and parsing to integer
				if (csvValue[21].matches("\\d+")) val = Integer.parseInt(csvValue[21]);
		//Write key and value pair
				context.write(new Text(keyVal), new IntWritable(val));
			}

	}

}