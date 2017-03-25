package BigData.CommonFollower;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.*;

public class CommonFollowerReducer2 extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context)
		      throws IOException, InterruptedException {
		String out = "{";
		for (Text val: values){
			out = out + val.toString() + ", ";
		}
		out = out.substring(0,out.length() - 2);
		out = out + "}";
		context.write(key, new Text(out));
	}

}
