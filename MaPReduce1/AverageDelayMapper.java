import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;

public class AverageDelayMapper extends
Mapper<LongWritable, Text, Text, FloatWritable> {
	public void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException {
			String[] csvValue = value.toString().split(",");
			if (csvValue[0].matches("\\d+")){
				float depDelay = 0;
				if (csvValue[15].matches("^(-?)\\d+")){
					depDelay = Float.parseFloat(csvValue[15]);
				}
				System.out.println("key: " + csvValue[8] + ", " + depDelay);
				context.write(new Text(csvValue[8]), new FloatWritable(depDelay));
			}

	}

}
