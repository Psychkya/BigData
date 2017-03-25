package BigData.LastAccess;
import org.apache.hadoop.io.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class IPAddressTimeComparator extends WritableComparator{
	/*
	 * custom sort to sort first by IP address and then by access time (descending)
	 */
	
	public IPAddressTimeComparator(){
		super(IPDatePair.class,true);
	}
	
	public int compare(WritableComparable k1, WritableComparable k2){
		IPDatePair key1 = (IPDatePair) k1;
		IPDatePair key2 = (IPDatePair) k2;
		int c = key1.getIPAddress().compareTo(key2.getIPAddress());
		if(c == 0){
			Date dt1 = new Date ();
			Date dt2 = new Date ();
			dt1 = ConvertDate(key1.getAccessTime());
			dt2 = ConvertDate(key2.getAccessTime());
			return -dt1.compareTo(dt2);
		}else{
			return c;
		}
	}
	
	private static Date ConvertDate (Text dt){
		Date date = new Date();
		try {
		String[] parseDate = dt.toString().split(" ");
		String dtformat = parseDate[0].replace("[", "");
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
		date = formatter.parse(dtformat);
		}catch (ParseException e){
			e.printStackTrace();
			//System.exit(1);
		}
		return date;
		
	}

}
