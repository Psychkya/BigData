package BigData.oneStopFlight;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class OneStopFlightDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {	  
    	int exitCode = ToolRunner.run(new Configuration(), new OneStopFlightDriver(),args);
    System.exit(exitCode);
  }
	
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
		      System.err.println("Usage: OneStopFlightDriver <input path> <output path>");
		      System.exit(-1);
		    }
		//Initializing the map reduce job
		Job job= new Job(getConf());
		job.setJarByClass(OneStopFlightDriver.class);
		job.setJobName("OneStopFlight");
		

		//Setting the input and output paths.The output file should not already exist. 
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Setting the mapper, reducer, and combiner classes
		job.setMapperClass(OneStopFlightMapper.class);
		job.setReducerClass(OneStopFlightReducer.class);
		//job.setCombinerClass(WordCountReducer.class);

		//Setting the format of the key-value pair to write in the output file.
		//Setting the format of mapper class as well
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		//Submit the job and wait for its completion
		return(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
