	package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ErrorRateMapper extends Mapper<Object, Text, Text, FloatArrayWritable> {

	// Create a hadoop text object to store words
	private Text word = new Text();

	private FloatArrayWritable vals = new FloatArrayWritable();

	@Override
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] tok = value.toString().split(",");

		
		String taxiID = tok[0];
		String[] tok = value.toString().split(",");
        String startHourString = tok[2].split(" ")[1].split(":")[0];
        String endHourString = tok[3].split(" ")[1].split(":")[0];
        String money_str = tok[16];
        String seconds_str = tok[4];

        IntWritable startHour;
        IntWritable endHour;

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
			float seconds = Float.parseFloat(seconds_str);
            if (seconds == 0) {
                return;
            }
		} catch (Exception e) {
			return;
		}
		
		float error = 0;
		if (tok[6].length() == 0 || tok[7].length() == 0) {
			error = 1;
        } else if (tok[8].length() == 0 || tok[9].length() == 0) {
            error = 1;
        } else {
            float pickLong = Float.parseFloat(tok[6]);
            float pickLat = Float.parseFloat(tok[7]);
            float dropLong = Float.parseFloat(tok[8]);
            float dropLat = Float.parseFloat(tok[9]);
            if (pickLong == 0 || pickLat == 0) {
                error = 1;
            } else if (dropLong == 0 || dropLat == 0) {
                error = 1;
            }
        }

		FloatWritable[] vals = new FloatWritable[2];
		store[0] = new FloatWritable(error);
		store[1] = new FloatWritable(1);
		vals.set(store);

		// Set the driverID text and emit the ArrayWritable
		word.set(taxiID);
		context.write(taxiID, vals);
	}
}