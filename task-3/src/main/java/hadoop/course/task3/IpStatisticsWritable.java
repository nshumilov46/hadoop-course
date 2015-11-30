package hadoop.course.task3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IpStatisticsWritable implements Writable {

    private LongWritable totalBytesTransferred;
    private LongWritable totalTransfers;
    private DoubleWritable avgBytesTransferred;

    public IpStatisticsWritable() {
        this.totalBytesTransferred = new LongWritable();
        this.totalTransfers = new LongWritable();
        this.avgBytesTransferred = new DoubleWritable();
    }

    public IpStatisticsWritable(LongWritable totalBytesTransferred, LongWritable totalTransfers) {
        this.totalBytesTransferred = totalBytesTransferred;
        this.totalTransfers = totalTransfers;
        this.avgBytesTransferred = new DoubleWritable();
    }

    public IpStatisticsWritable(LongWritable totalBytesTransferred, LongWritable totalTransfers,
                                DoubleWritable avgBytesTransferred) {
        this.totalBytesTransferred = totalBytesTransferred;
        this.totalTransfers = totalTransfers;
        this.avgBytesTransferred = avgBytesTransferred;
    }

    public LongWritable getTotalBytesTransferred() {
        return totalBytesTransferred;
    }

    public void setTotalBytesTransferred(LongWritable totalBytesTransferred) {
        this.totalBytesTransferred = totalBytesTransferred;
    }

    public LongWritable getTotalTransfers() {
        return totalTransfers;
    }

    public void setTotalTransfers(LongWritable totalTransfers) {
        this.totalTransfers = totalTransfers;
    }

    public DoubleWritable getAvgBytesTransferred() {
        return avgBytesTransferred;
    }

    public void setAvgBytesTransferred(DoubleWritable avgBytesTransferred) {
        this.avgBytesTransferred = avgBytesTransferred;
    }

    public void write(DataOutput dataOutput) throws IOException {
        totalBytesTransferred.write(dataOutput);
        totalTransfers.write(dataOutput);
        avgBytesTransferred.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        totalBytesTransferred.readFields(dataInput);
        totalTransfers.readFields(dataInput);
        avgBytesTransferred.readFields(dataInput);
    }

    @Override
    public String toString() {
        return avgBytesTransferred.toString() + ',' + totalBytesTransferred.toString();
    }
}
