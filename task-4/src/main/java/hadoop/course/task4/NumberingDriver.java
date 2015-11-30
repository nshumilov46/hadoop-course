package hadoop.course.task4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NumberingDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: task-4 <input_directory> <output_directory>");
            System.exit(-1);
        }
        int res = ToolRunner.run(new Configuration(), new NumberingDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(getClass());

        job.setMapperClass(NumberingMapper.class);
        job.setGroupingComparatorClass(NumberingComparator.class);
        job.setPartitionerClass(NumberingPartitioner.class);
        job.setReducerClass(NumberingReducer.class);

        job.setMapOutputKeyClass(ByteWritable.class);
        job.setMapOutputValueClass(NumberingWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 1 : 0;
    }
}
