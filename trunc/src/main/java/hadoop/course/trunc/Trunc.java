package hadoop.course.trunc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class Trunc {

    public static void main(String[] args) throws IOException {

        if(args.length < 3){
            System.out.println("Usage: trunc <input file> <output file> <line count>");
            System.exit(-1);
        }

        String inputUri = args[0], outputUri=args[1];
        long lineCount = Long.parseLong(args[3]);

        FileSystem fs = FileSystem.get(new Configuration());

        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(inputUri))));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputUri))));

        for(; lineCount>0; lineCount--) {
            bw.write(br.readLine());
            bw.newLine();
        }

        br.close();
        bw.close();

    }
}
