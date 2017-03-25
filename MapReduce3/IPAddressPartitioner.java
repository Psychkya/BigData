package BigData.LastAccess;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class IPAddressPartitioner extends Partitioner<IPDatePair, Text>{
	
	@Override
	public int getPartition(IPDatePair key, Text value, int numReducers) {
		return Math.abs(key.getIPAddress().hashCode()% numReducers);
	}
}

