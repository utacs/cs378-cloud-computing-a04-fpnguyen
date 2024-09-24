package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

			// TASK 1
			Job job1 = new Job(conf, "Driver1");
			job1.setJarByClass(Driver.class);

			// specify a Mapper
			job1.setMapperClass(ErrorCountMapper.class);

			// specify a Reducer
			job1.setReducerClass(ErrorCountReducer.class);

			// specify output types
			job1.setOutputKeyClass(IntWritable.class);
			job1.setOutputValueClass(IntWritable.class);

			// specify input and output directories
			FileInputFormat.addInputPath(job1, new Path(args[0]));
			job1.setInputFormatClass(TextInputFormat.class);

			FileOutputFormat.setOutputPath(job1, new Path(args[2]));
			job1.setOutputFormatClass(TextOutputFormat.class);


			if (!job1.waitForCompletion(true)) {
				return 1;
			}

			// TASK 2



			// TASK 3

			// Job job4 = new Job(conf, "Driver3");
			// job4.setJarByClass(Driver.class);

			// // specify a Mapper
			// job4.setMapperClass(EarningRateMapper.class);

			// // specify a Reducer
			// job4.setReducerClass(EarningRateReducer.class);

			// // specify output types
			// job4.setOutputKeyClass(Text.class);
			// job4.setOutputValueClass(Text.class);
			// job4.setMapOutputValueClass(FloatArrayWritable.class);

			// // specify input and output directories
			// FileInputFormat.addInputPath(job4, new Path(args[0]));
			// job4.setInputFormatClass(TextInputFormat.class);

			// FileOutputFormat.setOutputPath(job4, new Path(args[1] + "job4"));
			// job4.setOutputFormatClass(TextOutputFormat.class);


			// if (!job4.waitForCompletion(true)) {
			// 	return 1;
			// }

			// Job job5 = new Job(conf, "TopK3");
			// job5.setJarByClass(Driver.class);

			// // specify a Mapper
			// job5.setMapperClass(TopKEarningMapper.class);

			// // specify a Reducer
			// job5.setReducerClass(TopKEarningReducer.class);

			// // specify output types
			// job5.setOutputKeyClass(Text.class);
			// job5.setOutputValueClass(Text.class);
			// job5.setMapOutputValueClass(FloatArrayWritable.class);

			// // set the number of reducer to 1
			// job5.setNumReduceTasks(1);

			// // specify input and output directories
			// FileInputFormat.addInputPath(job5, new Path(args[1] + "job4"));
			// job5.setInputFormatClass(KeyValueTextInputFormat.class);

			// FileOutputFormat.setOutputPath(job5, new Path(args[2] + "job5"));
			// job5.setOutputFormatClass(TextOutputFormat.class);

			return (job1.waitForCompletion(true) ? 0 : 1);

		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during driver job.");
			e.printStackTrace();
			return 2;
		}
	}
}
