package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ErrorRateReducer extends Reducer<Text, FloatArrayWritable, Text, Text> {

    @Override
    public void reduce(Text text, Iterable<FloatArrayWritable> values, Context context)
            throws IOException, InterruptedException {

        float error = 0;
        float total = 0;
        for (FloatArrayWritable value : values) {
            error += ((FloatWritable) value.get()[0]).get();
            total += ((FloatWritable) value.get()[1]).get();
        }

        // Create a string representation for delay and count
        String outputValue = error + "," + total;
        context.write(text, new Text(outputValue));
    }
}