package hadoop.course.task4;


import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NumberingPartitioner extends Partitioner<ByteWritable, NumberingWritable> {

    public static int partitionForValue(NumberingWritable value, int numPartitions) {
        return (value.getValue().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }

    @Override
    public int getPartition(ByteWritable key, NumberingWritable value, int numPartitions) {
        if (key.get() == (byte) 0) {
            return value.getPartition();
        } else {
            return partitionForValue(value, numPartitions);
        }
    }

}
