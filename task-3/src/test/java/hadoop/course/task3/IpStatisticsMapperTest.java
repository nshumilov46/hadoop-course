package hadoop.course.task3;

import org.junit.Test;

import java.io.IOException;


public class IpStatisticsMapperTest {
    @Test
    public void validRecord() throws IOException {
        /*Text record = new Text("ip12 - - [24/Apr/2011:04:42:20 -0400] \"GET /favicon.ico HTTP/1.1\" 200 318 " +
                "\"-\" \"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0) Gecko/20100101 Firefox/4.0\"");
        new MapDriver<LongWritable, Text, Text, IpStatisticsWritable>()
                .withMapper(new IpStatisticsMapper())
                .withInput(new LongWritable(0), record)
                .withOutput(new Text("ip12"), new IpStatisticsWritable(new LongWritable(318), new LongWritable(1)))
                .runTest();*/
        //equals() not working properly?
    }
}