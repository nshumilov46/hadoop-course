package hadoop.course.task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IpStatisticsDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.out.println("Usage: * <input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        int res = ToolRunner.run(conf, new IpStatisticsDriver(), args);
        System.exit(res);

    }

    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();

        Job job = Job.getInstance(conf, "ipstat");
        job.setJarByClass(IpStatisticsDriver.class);

        job.setMapperClass(IpStatisticsMapper.class);
        job.setCombinerClass(IpStatisticsReducer.class);
        job.setReducerClass(IpStatisticsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IpStatisticsWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IpStatisticsWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        // FileOutputFormat.setOutputPath(job, new Path(args[1]));

        SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
