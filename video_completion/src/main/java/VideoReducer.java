//22BCE0511 Vishnu P K
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class VideoReducer extends Reducer<Text, IntWritable, Text, Text> {
    private static final Map<String, Integer> VIDEO_LENGTHS = new HashMap<>();
    private static final double COMPLETION_RATE_THRESHOLD = 40.0;
    static {
        VIDEO_LENGTHS.put("v001", 320);
        VIDEO_LENGTHS.put("v002", 240);
        VIDEO_LENGTHS.put("v003", 70);
        VIDEO_LENGTHS.put("v004", 500);
        VIDEO_LENGTHS.put("v005", 160);
        VIDEO_LENGTHS.put("v006", 380);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        String videoId = key.toString();
        int totalDuration = 0;
        int count = 0;
        for (IntWritable val : values) {
            totalDuration += val.get();
            count++;
        }
        if (count == 0) {
            return;
        }
        double avgWatched = (double) totalDuration / count;
        int length = VIDEO_LENGTHS.get(videoId);
        if (length <= 0) {
            context.write(key, new Text(String.format("Avg: %.2fs, Rate: Error (length is zero)", avgWatched)));
            return;
        }
        double completionRate = (avgWatched * 100.0) / length;
        if (completionRate < COMPLETION_RATE_THRESHOLD) {
            context.write(key, new Text(String.format("Avg: %.2fs, Rate: %.2f%% (LOW Completion rate!<40)", avgWatched, completionRate)));
        }
    }
}
