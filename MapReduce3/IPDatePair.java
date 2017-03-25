package BigData.LastAccess;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.text.*;

import org.apache.hadoop.io.*;

public class IPDatePair implements WritableComparable<IPDatePair>{
	/*
	 * Creates a composite key with IP address and access time
	 */
	
	private Text ipAddress;
	private Text accessTime;
	
	public IPDatePair(){
		
		ipAddress = new Text ();
		accessTime = new Text ();
	}
	
	public IPDatePair (String IP, String accessDt){
		ipAddress = new Text(IP);
		accessTime = new Text (accessDt);
	}
	
	public void readFields(DataInput in) throws IOException {
		ipAddress.readFields(in);
		accessTime.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException {
		ipAddress.write(out);
		accessTime.write(out);
	}
	
	public int compareTo(IPDatePair otherPair){
		int c= ipAddress.compareTo(otherPair.ipAddress);
		Date dt1 = new Date();
		Date dt2 = new Date();
		if (c!=0){
			return c;
		}
		else{
			dt1 = ConvertDate(accessTime);
			dt2 = ConvertDate(otherPair.accessTime);
			return dt1.compareTo(dt2);
		}
	}
	
	public Text getIPAddress(){
		return ipAddress;
	}
	
	public Text getAccessTime(){
		return accessTime;
	}
	
	public void setIPAddress(Text ipAddress) {
		this.ipAddress = ipAddress;
	}
	
	public void setAccessTime(Text accessTime) {
		this.accessTime = accessTime;
	}
	
	private static Date ConvertDate (Text dt){
		/*
		 * convert Text string to date format
		 */

		String[] parseDate = dt.toString().split(" ");
		String dtformat = parseDate[0].replace("[", "");
		Date date = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
		try {
			date = formatter.parse(dtformat);
		} catch (ParseException e){
			e.printStackTrace();
		}
		
		return date;
		
	}

}
