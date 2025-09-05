//22BCE0511 Vishnu P K
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VideoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        if (line.startsWith("videoID")) return;

        String[] parts = line.split(",");
        if (parts.length < 3) return;

        String videoId = parts[0].trim();
        int duration = Integer.parseInt(parts[2].trim());

        context.write(new Text(videoId), new IntWritable(duration));
    }
}