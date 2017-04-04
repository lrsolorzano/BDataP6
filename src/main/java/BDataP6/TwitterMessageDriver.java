package BDataP6;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

/**
 * Created by leo on 04-03-17.
 */
public class TwitterMessageDriver {

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();
        if (args.length != 2) {
            System.err.println("Usage: TwitterMessageDriver <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(BDataP6.TwitterMessageDriver.class);
        job.setJobName("Count Message_by_Tweets");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(BDataP6.TwitterMessageMapper.class);
        job.setReducerClass(BDataP6.TwitterMessageReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}