package BigData.CommonFollower;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;

public class CommonFollowerMapper1 extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException {
		String[] inText = value.toString().split("\\s+");
		context.write(new Text(inText[0]), new Text(inText[1]));
	}

}
