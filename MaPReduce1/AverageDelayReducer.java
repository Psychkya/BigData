import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.*;

public class AverageDelayReducer extends
Reducer<Text, FloatWritable, Text, FloatWritable> {

	public void reduce(Text key, Iterable<FloatWritable> values, Context context)
		      throws IOException, InterruptedException {
		    float avDelay = 0;
		    double sum = 0;
		    int count = 0;
		    for (FloatWritable value : values) {
		      sum += value.get();
		      count++;
		    }
		    avDelay = (float)sum/count;
		    context.write(key, new FloatWritable(avDelay));
		  }
}
