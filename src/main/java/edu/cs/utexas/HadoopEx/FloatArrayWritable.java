package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

public class FloatArrayWritable extends ArrayWritable {
    public FloatArrayWritable() {
        super(FloatWritable.class);
    }

    public FloatArrayWritable(FloatWritable[] values) {
        super(FloatWritable.class, values);
    }
    
    @Override
    public String toString() {
        Writable[] array = get();
        if (array.length < 2) {
            return "Not enough elements to perform division.";
        }

        float money = ((FloatWritable) array[0]).get();
        float seconds = ((FloatWritable) array[1]).get();

        // Handle division by zero
        if (seconds == 0) {
            return "Division by zero error.";
        }

        // Return the result of delay/count
        return String.valueOf(money / seconds);
    }
}