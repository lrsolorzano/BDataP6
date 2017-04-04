package BDataP6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by leo on 04-03-17.
 */
public class TwitterMessageReducer extends Reducer<Text, Text,Text, Text>  {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int count = 0;
        String result = "[ ";

        for (Text value : values){
            result = result + value;
            result = result + " |*| ";
            count = count +1;
        }

        Integer.toString(count);
        result = result + count;
        result = result + " ]";

        context.write(key, new Text(result));

    }


}
