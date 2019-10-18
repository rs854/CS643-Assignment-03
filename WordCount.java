import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class WordCount {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

    private Text word = new Text();
    private Text state = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
      state.set(fileName);
      StringTokenizer itr = new StringTokenizer(value.toString());

      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken().toLowerCase().trim());

        String wordStr = word.toString();

        if (wordStr.equals("education") || wordStr.equals("politics") || wordStr.equals("sports")
            || wordStr.equals("agriculture")) {
          context.write(state, word);
        }
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      int educationSum = 0;
      int politicsSum = 0;
      int sportsSum = 0;
      int agricultureSum = 0;
      String valStr = "";
      String debug = "";
      int sum = 0;

      for (Text val : values) {
        valStr = val.toString();
        if (valStr.equals("education"))
        {
          educationSum++;
        }
        if (valStr.equals("politics"))
        {
          politicsSum++;
        }
        if (valStr.equals("sports"))
        {
          sportsSum++;
        }
        if (valStr.equals("agriculture"))
        {
          agricultureSum++;
        }
      }

      //result.set(String.format("%d - %s", sum, debug));
      result.set(String.format("%d %d %d %d", educationSum, politicsSum, sportsSum, agricultureSum));
      //result.set(debug);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
