import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {

    private FileSplit split;
    private Set<String> stopWords;
    private Pattern regex;
    private Matcher match;
    private Text path;
    private boolean DEBUG_MODE;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

        DEBUG_MODE = context.getConfiguration().getBoolean("DEBUG_MODE", false);

        stopWords = Stream.of(
                "the", "of", "and", "a", "to", "in", "is", "you", "that", "if", "but", "or", "my", "his", "her", "he",
                "she", "i", "with", "for", "it", "this", "by", "as", "was", "had", "not", "him", "be", "at", "on", "your"
        ).collect(Collectors.toCollection(HashSet::new));

        regex = Pattern.compile("[a-zA-Z]+");
        split = (FileSplit)context.getInputSplit();
        // TODO print this path, it may contain the GCP bucket info
        //   path.name.split("input/")[1] ??
        path = new Text(split.getPath().toString().split("input/")[1]);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String word = null;

        match = regex.matcher(line);

        int i = 0;
        while(match.find()) {
            word = match.group().toLowerCase();

            if (stopWords.contains(word) || word.length() == 1)  // Only add this word if it's not a stop word or
                continue;                                        // if len is 1 because Shakespeare adds 'd to end of words and that gets chopped off by the regex

            if (i++ % 100 == 0 && DEBUG_MODE)
                System.out.println("map writing to context, key: " + word + " val: " + path);
            context.write(new Text(word), path);
        }

    }

}
