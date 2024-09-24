package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


import org.apache.log4j.Logger;


public class TopKErrorMapper extends Mapper<Text, Text, Text, FloatArrayWritable> {

	private Logger logger = Logger.getLogger(TopKErrorMapper.class);


	private PriorityQueue<TaxiErrors> pq;

	public void setup(Context context) {
		pq = new PriorityQueue<>();

	}

	/**
	 * Reads in results from the first job and filters the topk results
	 *
	 * @param key
	 * @param value a float value stored as a string
	 */
	@Override
	public void map(Text key, Text val, Context context)
			throws IOException, InterruptedException {
		System.out.println("before");
		String[] parts = val.toString().split(",");
		for (int i = 0; i < parts.length; i++){
			System.out.println(parts[i]);
		}
		float error = Float.parseFloat(parts[0]);
		float total = Float.parseFloat(parts[1]);

		pq.add(new TaxiErrors(new Text(key), new FloatWritable(error), new FloatWritable(total)) );

		if (pq.size() > 5) {
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		
		while (pq.size() > 0) {
			TaxiErrors earn = pq.poll();
			context.write(earn.getTaxiID(), earn.getVals());
			logger.info("TopKMapper PQ Status: " + pq.toString());
		}
	}

}