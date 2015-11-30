package hadoop.course.task1;

import org.apache.hadoop.conf.Configuration;
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

        if(args.length < 2){
            System.out.println("Usage: ipinyou <input file> <output file>");
        }

        //TODO Add recursive folder traversal

        String inputUri = args[0], outputUri = args[1];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(inputUri), conf);

        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(inputUri))));

        //First thing that came to mind, sorry

        HashMap<String, Long> occurrences = new HashMap<>();
        IPinYouParser parser = new IPinYouParser();
        String line;
        String id;
        Long prev;

        while((line = br.readLine()) != null){
            if(parser.parse(line)) {
                id = parser.getIPinYouId();
                prev = occurrences.get(id);
                occurrences.put(id, prev == null? 1 : prev + 1);
            }
        }

        ArrayList<HashMap.Entry<String, Long>> entries = new ArrayList<>(occurrences.entrySet());

        Comparator<HashMap.Entry<String, Long>> descValueComparator =
                (e1, e2) -> e1.getValue() < e2.getValue() ? 1 : e1.getValue() > e2.getValue() ? -1 : 0;

        Collections.sort(entries, descValueComparator);

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputUri))));

        for (HashMap.Entry<String, Long> e : entries){
            bw.write(e.getKey() + '\t' + e.getValue());
            bw.newLine();
        }
    }
}
