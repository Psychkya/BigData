package BigData.oneStopFlight;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;

public class OneStopFlightMapper extends
Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException {
		//Splitting the comma separated values to an array
			String[] csvValue = value.toString().split(",");
		//Check that the starting value is numeric - this will eliminate the header record
		//Ensure schedule arrival and departure times are numeric. Eliminate records that
			if (csvValue[0].matches("\\d+") && csvValue[5].matches("\\d+") && csvValue[7].matches("\\d+")){
				//Two sets of key value pair for each record - One for arrival location and one for destination
					String outKey = csvValue[8] + "," + csvValue[0] + "," + csvValue[1] + "," +
							csvValue[2] + "," + csvValue[16];
					String val = csvValue[9] + "," + csvValue[17] + "," + csvValue[7] + "," +
							csvValue[5] + "," + "O";
					context.write(new Text(outKey), new Text(val));
					outKey = csvValue[8] + "," + csvValue[0] + "," + csvValue[1] + "," +
							csvValue[2] + "," + csvValue[17];
					val = csvValue[9] + "," + csvValue[16] + "," + csvValue[7] + "," +
							csvValue[5] + "," + "D";
					context.write(new Text(outKey), new Text(val));

				}

	}

}