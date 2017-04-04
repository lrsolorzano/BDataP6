package BDataP6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.*;

import java.io.IOException;

/**
 * Created by leo on 04-03-17.
 */
public class TwitterMessageMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);

            context.write(new Text(status.getUser().getScreenName()), new Text(status.getText()));

        }
        catch(TwitterException e){

        }
    }
}