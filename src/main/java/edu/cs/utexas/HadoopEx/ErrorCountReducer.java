package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ErrorCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int errors = 0;
        for (IntWritable value : values) {
            errors += value.get();
        }

        context.write(key, new IntWritable(errors));
    }
}