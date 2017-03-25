package BigData.AverageDelay;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class AverageDelayPair implements Writable {
	/*
	 * class to create a composite value. Will pass sum and count as value
	 */
	
	private FloatWritable partialSum;
	private IntWritable partialCount;
	
	public AverageDelayPair()
	{
		partialSum = new FloatWritable(0);
		partialCount=new IntWritable(0);
	}
	
	public AverageDelayPair(float sum, int count)
	{
		this.partialSum= new FloatWritable(sum);
		this.partialCount = new IntWritable(count);
	}
	
	public float getPartialSum()
	{
		return this.partialSum.get();
	}

	public int getPartialCount()
	{
		return this.partialCount.get();
	}

	public void setPartialSum(float sum)
	{
		this.partialSum=new FloatWritable(sum);
	}

	public void setPartialCount(int count)
	{
		this.partialCount= new IntWritable(count);
	}
	
	public void readFields(DataInput in) throws IOException {
		partialSum.readFields(in);
		partialCount.readFields(in);
		
	}
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		partialSum.write(out);
		partialCount.write(out);
	}

}
