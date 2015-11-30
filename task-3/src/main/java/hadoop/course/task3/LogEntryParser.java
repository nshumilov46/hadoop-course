package hadoop.course.task3;

import org.apache.hadoop.io.Text;

public class LogEntryParser {

    private String ip;
    private long bytesTransferred;

    public String getIp() {
        return ip;
    }

    public long getBytesTransfered() {
        return bytesTransferred;
    }

    public boolean parse(String record) {
        String[] tokens = record.split("\\s+");
        ip = tokens[0];
        try {
            bytesTransferred = Long.parseLong(tokens[9]);
        } catch (NumberFormatException ex) {
            return false;
        }
        return true;
    }

    public boolean parse(Text record) {
        return parse(record.toString());
    }
}
