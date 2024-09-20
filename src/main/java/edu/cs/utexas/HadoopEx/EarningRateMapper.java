package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EarningRateMapper extends Mapper<Text, Text, Text, FloatArrayWritable> {

	// Create a hadoop text object to store words
	private Text word = new Text();

	private FloatArrayWritable vals = new FloatArrayWritable();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] tok = value.toString().split(",");
		String driverID = tok[0];
        System.out.println(driverID + " hello??");
		String money_str = tok[16];
        String seconds_str = tok[4];

		FloatWritable money;
        FloatWritable seconds;
		try {
			money = new FloatWritable(Float.parseFloat(money_str));
            seconds = new FloatWritable(Float.parseFloat(seconds_str));
			if (seconds.get() == 0) {
				return;
			}

		} catch (Exception e) {
			return;
		}
		
		FloatWritable[] store = new FloatWritable[2];
		store[0] = money;
		store[1] = seconds;
		vals.set(store);

		// Set the driverID text and emit the ArrayWritable
		word.set(driverID);
		context.write(word, vals);
	}
}