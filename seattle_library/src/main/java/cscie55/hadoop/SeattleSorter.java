package cscie55.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class SeattleSorter {

    public static class MySorterMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        private Text title = new Text();

        public void map(Object key, Text value,
                        Context context
        ) throws IOException, InterruptedException {

            String line = value.toString().trim();

            String[] tokens = line.split("\\s+");
            String titleValue = line.substring(0, line.indexOf(tokens[tokens.length - 1]));
            int count = Integer.parseInt(tokens[tokens.length - 1].trim());
            title.set(titleValue);
            context.write(new IntWritable(count), title);
        }
    }

    public static class MySorterReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        private Text title = new Text();
        private IntWritable count = new IntWritable();

        public void reduce(IntWritable key, Iterator<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            while (values.hasNext()) {
                title.set(values.next());
                count = key;
            }
            context.write(count, title);
        }
    }

    public static void main(String[] args) throws Exception {


        String output = new String("target/") + args[1];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count sorter");
        job.setJarByClass(SeattleSorter.class);
        job.setMapperClass(MySorterMapper.class);
        job.setCombinerClass(MySorterReducer.class);
        job.setReducerClass(MySorterReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(output + "/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path(new String("target/") + args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
