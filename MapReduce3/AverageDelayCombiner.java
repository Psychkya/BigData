package BigData.AverageDelay;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class AverageDelayCombiner extends
Reducer<Text,AverageDelayPair, Text, AverageDelayPair> {
	public void reduce(Text key, Iterable<AverageDelayPair> values, Context context)
				throws IOException, InterruptedException {
		/*
		 * combiner to write out partial sum and count
		 */
			float sum = 0;
			int count=0;
			for (AverageDelayPair value : values) {
					sum += value.getPartialSum();
					count+= value.getPartialCount();
			}
			context.write(key, new AverageDelayPair(sum,count));
	} 

}