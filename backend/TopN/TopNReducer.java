import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class TopNReducer extends Reducer<Text, LongWritable, LongWritable, Text> {

    private static int N; // TODO read arg
    private TreeMap<String, Long> tMap2; // <NUM_OCCURRENCES, WORD>


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        tMap2 = new TreeMap<>();

        Configuration config = context.getConfiguration();

        N = Integer.parseInt(config.get("N"));

        if (N == 0) {
            System.err.println("Must Specify an N Value");
            System.exit(1);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        String word = key.toString();

        long l = 0;
        for(LongWritable val: values){
            l += val.get();
        }

        tMap2.put(word, l);
    }

    private static TreeMap<Long, String> invertMap(TreeMap<String, Long> map) {
        TreeMap<Long, String> swapped = new TreeMap<>(Collections.reverseOrder());

        for (Map.Entry<String, Long> e: map.entrySet())
            swapped.put(e.getValue(), e.getKey());

        return swapped;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        TreeMap<Long, String> swapped = invertMap(tMap2);

        String s;
        int i = 0;
        long l;

        // Iterate through the results TreeMap just created and write the results to the context.
        for (Map.Entry<Long, String> e: swapped.entrySet()) {
            if (++i > N)
                break;
            context.write(new LongWritable(e.getKey()), new Text(e.getValue()));
        }

    }
}
