package edu.cs.utexas.HadoopEx;

// import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
// import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
// import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.PriorityQueue;
// import java.util.Iterator;

public class TopKEarningReducer extends Reducer<Text, FloatArrayWritable, Text, Text> {

    private PriorityQueue<DriverEarnings> pq = new PriorityQueue<DriverEarnings>(5);;

    private Logger logger = Logger.getLogger(TopKEarningReducer.class);

    // public void setup(Context context) {
    //
    // pq = new PriorityQueue<WordAndCount>(10);
    // }

    /**
     * Takes in the topK from each mapper and calculates the overall topK
     * 
     * @param text
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<FloatArrayWritable> values, Context context)
            throws IOException, InterruptedException {

        // A local counter just to illustrate the number of values here! doesn't work
        // int counter = 0 ;

        // size of values is 1 because key only has one distinct value
        for (FloatArrayWritable tup : values) {

            Writable money = tup.get()[0];
            Writable seconds = tup.get()[1];

            logger.info("Reducer Text: numFlights is " + tup.get()[1]);
            logger.info("Reducer Text: Add this item  "
                    + new DriverEarnings(key, (FloatWritable) money, (FloatWritable)seconds).toString());

            pq.add(new DriverEarnings(new Text(key), (FloatWritable) money, (FloatWritable)seconds));

            logger.info("Reducer Text: " + key.toString() + " , Count: " + tup.toString());
            logger.info("PQ Status: " + pq.toString());
        }

        // keep the priorityQueue size <= heapSize
        while (pq.size() > 10) {
            pq.poll();
        }

    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("TopKReducer cleanup cleanup.");
        logger.info("pq.size() is " + pq.size());

        List<DriverEarnings> values = new ArrayList<DriverEarnings>(10);

        while (pq.size() > 0) {
            values.add(pq.poll());
        }

        logger.info("values.size() is " + values.size());
        logger.info(values.toString());

        // reverse so they are ordered in descending order
        Collections.reverse(values);
        
        for (DriverEarnings value : values) {
            Text val = new Text(value.toString());
            context.write(value.getDriverID(), val);
            logger.info("TopKReducer - Top-10 Airlines are:  " + value.getDriverID() + "  Ratio:"
                    + value.getRatio());
        }

    }

}