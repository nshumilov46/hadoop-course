package hadoop.course.task1;

public class IPinYouParser {

    private String iPinYouId;

    public String getIPinYouId() {
        return iPinYouId;
    }

    public boolean parse(String record) {
        String[] tokens = record.split("\\t");
        if(tokens.length < 3)
            return false;
        iPinYouId = tokens[2];
        return true;
    }
}
