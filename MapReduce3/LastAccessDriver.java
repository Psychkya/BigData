package BigData.LastAccess;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LastAccessDriver extends Configured implements Tool{
	
	public static void main (String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),new LastAccessDriver(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
		      System.err.println("Usage: Last Access <input path> <output path>");
		      System.exit(-1);
		    }
		    //Initializing the map reduce job
		    Job job= new Job(getConf());
		    job.setJarByClass(LastAccessDriver.class);
		    job.setJobName("Last Access Time");

		    //Setting the input and output paths.The output file should not already exist. 
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		    //Setting the mapper, reducer, and combiner classes
		    job.setMapperClass(LastAccessMapper.class);
		    job.setReducerClass(LastAccessReducer.class);
		    
		    //Setting the output key value type of the mapper 
		    job.setMapOutputKeyClass(IPDatePair.class);
		    job.setMapOutputValueClass(Text.class);
		    
		  //Setting the type of the key-value pair to write in the output file.
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		   

		    //Setting the grouping and sorting comparator classes
		      job.setGroupingComparatorClass(IPAddressComparator.class);
		      job.setSortComparatorClass(IPAddressTimeComparator.class);
		    
		    //Setting the custom partitioner
		      job.setPartitionerClass(IPAddressPartitioner.class);

		    //Submit the job and wait for its completion
		    return(job.waitForCompletion(true) ? 0 : 1);
		  }

}
