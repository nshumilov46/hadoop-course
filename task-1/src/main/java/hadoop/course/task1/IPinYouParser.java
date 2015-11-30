package hadoop.course.task1;

public class IPinYouParser {

    private String iPinYouId;

    public String getIPinYouId() {
        return iPinYouId;
    }

    public boolean parse(String record) {
        String[] split = record.split("\\t");
        iPinYouId = split[2];
        return true;
    }
}
