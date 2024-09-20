package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EarningRateReducer extends Reducer<Text, FloatArrayWritable, Text, Text> {

    public void reduce(Text text, Iterable<FloatArrayWritable> values, Context context)
            throws IOException, InterruptedException {

        float money = 0;
        float seconds = 0;
        for (FloatArrayWritable value : values) {
            money += ((FloatWritable) value.get()[0]).get();
            seconds += ((FloatWritable) value.get()[1]).get();
        }

        // Create a string representation for delay and count
        String outputValue = money + "," + seconds;
        context.write(text, new Text(outputValue));
    }
}