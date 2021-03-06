
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class InvertedIndexDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        conf.setBoolean("DEBUG_MODE", false);

        // if less than two paths
        // provided will show error
        if (otherArgs.length < 2)
        {
            System.err.println("Error: please provide two paths");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "InvertedIndex");
        job.setJarByClass(InvertedIndexDriver.class);

        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Object.class);

        // TODO
        // job.setNumReduceTasks(1);


        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }   // End Main


}   // End Driver class
