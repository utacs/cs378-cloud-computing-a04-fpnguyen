package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.ArrayWritable;
// import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(res);
	}

	/**
	 * 
	 */
	public int run(String args[]) {
		try {
			Configuration conf = new Configuration();

			Job job = new Job(conf, "WordCount");
			job.setJarByClass(Driver.class);

			// specify a Mapper
			job.setMapperClass(EarningRateMapper.class);

			// specify a Reducer
			// job.setReducerClass(EarningRateReducer.class);

			// specify output types
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatArrayWritable.class);

			// specify input and output directories
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(TextInputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			if (!job.waitForCompletion(true)) {
				return 1;
			}

			System.out.println("job 1 done");

			// Job job2 = new Job(conf, "TopK");
			// job2.setJarByClass(Driver.class);

			// // specify a Mapper
			// job2.setMapperClass(TopKEarningMapper.class);

			// // specify a Reducer
			// job2.setReducerClass(TopKEarningReducer.class);

			// // specify output types
			// job2.setOutputKeyClass(Text.class);
			// job2.setOutputValueClass(Text.class);

			// // set the number of reducer to 1
			// job2.setNumReduceTasks(1);

			// // specify input and output directories
			// FileInputFormat.addInputPath(job2, new Path(args[1]));
			// job2.setInputFormatClass(KeyValueTextInputFormat.class);

			// FileOutputFormat.setOutputPath(job2, new Path(args[2]));
			// job2.setOutputFormatClass(TextOutputFormat.class);

			// return (job2.waitForCompletion(true) ? 0 : 1);

		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during driver job.");
			e.printStackTrace();
			return 2;
		}
		return -1;
	}
}
