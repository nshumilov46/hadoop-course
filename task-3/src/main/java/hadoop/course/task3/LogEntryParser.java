package hadoop.course.task3;

import org.apache.hadoop.io.Text;

public class LogEntryParser {

    private String ip;
    private String userAgent;
    private long bytesTransferred;

    public String getUserAgent() {
        return userAgent;
    }

    public String getIp() {
        return ip;
    }

    public long getBytesTransfered() {
        return bytesTransferred;
    }

    public boolean parse(String record) {
        String[] tokens = record.split("\\s+");
        if (tokens.length < 12)
            return false;
        ip = tokens[0];
        userAgent = tokens[11];
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
