package BigData.LastAccess;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.util.*;
public class LastAccessReducer extends Reducer<IPDatePair, Text, Text, Text>{
	
	public void reduce (IPDatePair key, Iterable<Text> values, Context context) throws IOException, 
			InterruptedException {
		Iterator<Text> it = values.iterator();
		if(it.hasNext()){
			context.write(key.getIPAddress(), values.iterator().next());
		}
		
	}

}
