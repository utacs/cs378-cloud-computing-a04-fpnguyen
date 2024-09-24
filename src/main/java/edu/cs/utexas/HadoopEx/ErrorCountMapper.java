package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ErrorCountMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] tok = value.toString().split(",");
        String startHourString = tok[2].split(" ")[1].split(":")[0];
        String endHourString = tok[3].split(" ")[1].split(":")[0];
        // String money_str = tok[16];
        String seconds_str = tok[4];

        IntWritable startHour;
        IntWritable endHour;

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		LocalDateTime startDT = LocalDateTime.parse(tok[2], formatter);
        LocalDateTime endDT = LocalDateTime.parse(tok[3], formatter);

        // error checking
        try {
            if (endDT.isBefore(startDT)) {
                return;
            }
            if (tok.length != 17) {
                return;
            }
            float sum = 0;
            for (int x = 11; x < 16; x++)
                sum += Float.parseFloat(tok[x]);
            float realSum = Float.parseFloat(tok[16]);
            if (!(sum + 0.05 > realSum) || !(sum - 0.05 < realSum) || sum > 500) {
                return;
            }
            // error checking for earnings
            float seconds = Float.parseFloat(seconds_str);
            if (seconds == 0) {
                return;
            }
        } catch (Exception e) {
            return;
        }

        try {
            startHour = new IntWritable(Integer.parseInt(startHourString) + 1);
            endHour = new IntWritable(Integer.parseInt(endHourString) + 1);
        } catch (Exception e) {
            return;
        }

        boolean a = tok[6].length() == 0 || tok[7].length() == 0;
        boolean b = tok[8].length() == 0 || tok[9].length() == 0;
        if (a) {
            context.write(startHour, new IntWritable(1));
        }
        if (b) {
            context.write(endHour, new IntWritable(1));
        }

        if ((a && !b) || (b && !a) || (!b && !a)) {
            if (!a) {
                float pickLong = Float.parseFloat(tok[6]);
                float pickLat = Float.parseFloat(tok[7]);
                if ((pickLong == 0 || pickLat == 0)) {
                    context.write(startHour, new IntWritable(1));
                }
            }
            if (!b) {
                float dropLong = Float.parseFloat(tok[8]);
                float dropLat = Float.parseFloat(tok[9]);
                if (dropLong == 0 || dropLat == 0) {
                    context.write(endHour, new IntWritable(1));
                }
            }
        }
    }
}
