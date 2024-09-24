package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EarningRateMapper extends Mapper<Object, Text, Text, FloatArrayWritable> {

	// Create a hadoop text object to store words
	private Text word = new Text();

	private FloatArrayWritable vals = new FloatArrayWritable();

	@Override
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] tok = value.toString().split(",");

		
		String driverID = tok[1];
		String money_str = tok[16];
        String seconds_str = tok[4];

		FloatWritable money;
        FloatWritable seconds;
		try {
			if (tok.length != 17){
				return;
			} 
			float sum = 0;
			for (int x = 11; x < 16; x++)
				sum += Float.parseFloat(tok[x]);
			float realSum = Float.parseFloat(tok[16]);
			if (!(sum + 0.05 > realSum) || !(sum - 0.05 < realSum) || sum > 500) {
				return;
			}
			money = new FloatWritable(Float.parseFloat(money_str));
            seconds = new FloatWritable(Float.parseFloat(seconds_str));
			if (seconds.get() == 0 || money_str.isEmpty() || seconds_str.isEmpty()) {
				return;
			}
		} catch (Exception e) {
			System.out.println(tok);
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