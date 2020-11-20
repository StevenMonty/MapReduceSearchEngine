import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**	
 * TODO
 * 
 * Always export the local top N, even if that does not result in the final result being 100% accurate
 * 
 * Separate jar files for Top N and Search Term, but there is a way to reuse the constructed indicies so they are only computed once
 * 
 * @author Steven Montalbano
 */


public class TopNMapper extends Mapper<Object, Text, Text, LongWritable> {
    private static int N;     // TODO read this arg
    private TreeMap<String, Long> tMap; // <WORD, NUM_OCCURRENCES>

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        tMap = new TreeMap<>();

        Configuration config = context.getConfiguration();

        N = Integer.parseInt(config.get("N"));

        if (N == 0) {
            System.err.println("Must Specify an N Value");
            System.exit(1);
        }

    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String val = value.toString().toLowerCase();

        // Check if the word has already been seen. If so, increment its existing count, else add count = 1 to the Map
        if (tMap.containsKey(val))
            tMap.put(val, tMap.get(val)+1);
        else
            tMap.put(val, 1L);
    }

    private static TreeMap<Long, String> invertMap(TreeMap<String, Long> map) {
        TreeMap<Long, String> swapped = new TreeMap<>(Collections.reverseOrder());

        for (Map.Entry<String, Long> e: map.entrySet())
            swapped.put(e.getValue(), e.getKey());

        return swapped;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        TreeMap<Long, String> swapped = invertMap(tMap);

        int i = 1;
        for (Map.Entry<Long, String> e: swapped.entrySet()) {
            if (i++ > N)
                break;
            context.write(new Text(e.getValue()), new LongWritable(e.getKey()));
        }

    }

}
