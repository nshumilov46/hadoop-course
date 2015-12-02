package hadoop.course.task3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IpStatisticsMapper extends Mapper<LongWritable, Text, Text, IpStatisticsWritable> {

    private LogEntryParser parser = new LogEntryParser();
    private LongWritable one = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        if (!parser.parse(value))
            return;

        String ip = parser.getIp();
        String userAgent = parser.getUserAgent();
        long bytesTransferred = parser.getBytesTransfered();

        if (userAgent.startsWith("\"Mozilla"))
            context.getCounter(UserAgent.MOZILLA).increment(1L);
        else if (userAgent.startsWith("\"Opera"))
            context.getCounter(UserAgent.OPERA).increment(1L);
        else
            context.getCounter(UserAgent.OTHER).increment(1L);

        context.write(new Text(ip), new IpStatisticsWritable(new LongWritable(bytesTransferred), one));
    }
}
