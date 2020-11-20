import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**	
 * TODO
 * 
 * Always export the local top N, even if that does not result in the final result being 100% accurate
 * 
 * Separate jar files for Top N and Search Term, but there is a way to reuse the constructed indicies so they are only computed once
 * 
 * @author Steven Montalbano
 */


public class LeastNMapper extends Mapper<Object, Text, Text, LongWritable> {
    private final static int N = 5;
    private TreeMap<String, Long> tMap; // <WORD, NUM_OCCURRENCES>

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        tMap = new TreeMap<>();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String val = value.toString();

        // Check if the word has already been seen. If so, increment its existing count, else add count = 1 to the Map
        if (tMap.containsKey(val))
            tMap.put(val, tMap.get(val)+1);
        else
            tMap.put(val, 1L);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        String s;
        long l;
        for (Map.Entry<String, Long> entry: tMap.entrySet()){
            s = entry.getKey();
            l = entry.getValue();

            context.write(new Text(s), new LongWritable(l));
        }

    }

}
