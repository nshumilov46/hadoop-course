package hadoop.course.task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

public class IdOccurrenceCounter {
    public static void main(String[] args) throws IOException {

        if (args.length < 2) {
            System.out.println("Usage: ipinyou <input directory> <output file>");
            System.exit(-1);
        }

        String inputUri = args[0], outputUri = args[1];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(inputUri), conf);

        //First thing that came to mind, sorry

        HashMap<String, Integer> occurrences = new HashMap<>();
        IPinYouParser parser = new IPinYouParser();
        String line;
        String id;
        Integer prev;

        FileStatus[] fileStatuses = fs.listStatus(new Path(inputUri));

        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
                while ((line = br.readLine()) != null) {
                    if (parser.parse(line)) {
                        id = parser.getIPinYouId();
                        prev = occurrences.get(id);
                        occurrences.put(id, prev == null ? 1 : prev + 1);
                    }
                }
                br.close();
            }
        }

        ArrayList<HashMap.Entry<String, Integer>> entries = new ArrayList<>(occurrences.entrySet());

        Comparator<HashMap.Entry<String, Integer>> descValueComparator = (e1, e2) -> e2.getValue() - e1.getValue();

        Collections.sort(entries, descValueComparator);

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputUri))));

        for (HashMap.Entry<String, Integer> e : entries) {
            bw.write(e.getKey() + '\t' + e.getValue());
            bw.newLine();
        }

        bw.close();
    }
}
