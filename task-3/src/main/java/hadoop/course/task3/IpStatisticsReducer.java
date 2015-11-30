package hadoop.course.task3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class IpStatisticsReducer extends Reducer<Text, IpStatisticsWritable, Text, IpStatisticsWritable> {
    @Override
    protected void reduce(Text key, Iterable<IpStatisticsWritable> values, Context context)
            throws IOException, InterruptedException {

        long totalBytesTransferred = 0;
        long counter = 0;

        for (IpStatisticsWritable value : values) {
            totalBytesTransferred += value.getTotalBytesTransferred().get();
            counter += value.getTotalTransfers().get();
        }

        context.write(key, new IpStatisticsWritable(new LongWritable(totalBytesTransferred),
                new LongWritable(counter), new DoubleWritable((double) totalBytesTransferred / counter)));
    }
}
