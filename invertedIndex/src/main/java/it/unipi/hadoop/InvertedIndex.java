package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.StringTokenizer;

public class InvertedIndex {

    public static class InvIndexMapper extends Mapper<LongWritable, Text, Text, FileCountData> {
        
        private static final Pattern CLEAN_TEXT = Pattern.compile("[^a-zA-Z0-9\\s]");
        private final Text outputKey = new Text();
        private final FileCountData outputValue = new FileCountData();
        private String filename;

        @Override
        protected void setup(Context context) {
            FileSplit split = (FileSplit) context.getInputSplit();
            filename = split.getPath().getName();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();
            line = CLEAN_TEXT.matcher(line).replaceAll(" ");
            
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                outputKey.set(itr.nextToken());
                outputValue.setCount(1);
                outputValue.setFilename(filename);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class InvIndexCombiner extends Reducer<Text, FileCountData, Text, FileCountData> {

        private final FileCountData combinedValue = new FileCountData();

        @Override
        protected void reduce(Text key, Iterable<FileCountData> values, Context context) throws IOException, InterruptedException {

            // Aggregate counts per filename in a local map
            Map<String, Long> localCounts = new HashMap<>();

            for (FileCountData fcd : values) {
                // Sum counts by filename
                localCounts.merge(fcd.getFilename(), fcd.getCount(), Long::sum);
            }

            for (Map.Entry<String, Long> entry : localCounts.entrySet()) {
                combinedValue.setFilename(entry.getKey());
                combinedValue.setCount(entry.getValue());
                context.write(key, combinedValue);
            }
        }
    }

    public static class InvIndexReducer extends Reducer<Text, FileCountData, Text, Text> {
        private final Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<FileCountData> values, Context context) throws IOException, InterruptedException {
            
            Map<String, Long> totalCounts = new HashMap<>();

            for (FileCountData fcd : values) {
                totalCounts.merge(fcd.getFilename(), fcd.getCount(), Long::sum);
            }

            // Build tab-separated "filename:count" list
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Long> entry : totalCounts.entrySet()) {
                if (sb.length() > 0) {
                    sb.append("\t");    // add separator only if it's not the first element
                }
                sb.append(entry.getKey()).append(":").append(entry.getValue());
            }
            result.set(sb.toString());

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: InvertedIndex <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "InvertedIndex");
        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(InvIndexMapper.class);
        job.setCombinerClass(InvIndexCombiner.class);
        job.setReducerClass(InvIndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FileCountData.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
    
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}