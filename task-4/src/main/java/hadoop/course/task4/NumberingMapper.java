package hadoop.course.task4;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NumberingMapper extends Mapper<LongWritable, Text, ByteWritable, NumberingWritable> {
    private long[] counters;
    private int numReduceTasks;

    private NumberingWritable outputValue = new NumberingWritable();
    private ByteWritable outputKey = new ByteWritable();

    protected void setup(Context context) throws IOException, InterruptedException {
        numReduceTasks = context.getNumReduceTasks();
        counters = new long[numReduceTasks];
        outputKey.set((byte) 1);
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        outputValue.setValue(value);
        context.write(outputKey, outputValue);
        counters[NumberingPartitioner.partitionForValue(outputValue, numReduceTasks)]++;
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        outputKey.set((byte) 0);
        for (int c = 0; c < counters.length - 1; c++) {
            if (counters[c] > 0) {
                outputValue.setCounter(c + 1, counters[c]);
                context.write(outputKey, outputValue);
            }
            counters[c + 1] += counters[c];
        }
    }
}
