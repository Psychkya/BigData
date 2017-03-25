package BigData.LastAccess;
import org.apache.hadoop.io.*;

public class IPAddressComparator extends WritableComparator{
	/*
	 * custom comparator to group keys only by IP Address
	 */
	
	public IPAddressComparator(){
		super(IPDatePair.class, true);
	}
	
	public int compare(WritableComparable k1, WritableComparable k2)
	{
		IPDatePair key1 = (IPDatePair) k1;
		IPDatePair key2= (IPDatePair) k2;
		return key1.getIPAddress().compareTo(key2.getIPAddress());
	}

}
