package BigData.AverageDelay;
import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.*;

public class AverageDelayReducer extends
Reducer<Text, AverageDelayPair, Text, FloatWritable> {

	public void reduce(Text key, Iterable<AverageDelayPair> values, Context context)
		      throws IOException, InterruptedException {
		    float avDelay = 0;
		    double sum = 0;
		    int count = 0;
		    for (AverageDelayPair value : values) {
		      sum += value.getPartialSum();
		      count += value.getPartialCount();
		    }
		    avDelay = (float)sum/count;
		    context.write(key, new FloatWritable(avDelay));
		  }
}
