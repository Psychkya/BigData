package BigData.AverageDelay;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;

public class AverageDelayMapper extends
Mapper<LongWritable, Text, Text, AverageDelayPair> {
	public void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException {
			String[] csvValue = value.toString().split(",");
			if (csvValue[0].matches("\\d+")){
				if (csvValue[15].matches("^(-?)\\d+")){
					context.write(new Text(csvValue[8]), new AverageDelayPair(Float.parseFloat(csvValue[15]), 1));
				}
			}
	}

}
