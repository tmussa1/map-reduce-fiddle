package cscie55.hadoop;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;


public class SeattleBooks {
    static Logger logger = LogManager.getLogger(SeattleBooks.class);

    public static class MyMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text title = new Text();


        public void map(Object key, Text value,
                        Context context
        ) throws IOException, InterruptedException {

            CSVParser parser = CSVParser.parse(value.toString(),
                    CSVFormat.RFC4180.withHeader("name", "type", "desc", "yyyy",
                            "MM", "dd", "title", "author", "summary", "short", "year"));

            CSVRecord record = parser.getRecords().get(0);
            title.set(record.get("title"));
            context.write(title, one);
        }
    }

    public static class MyReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private Text title = new Text();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            title.set(key);
            result.set(sum);
            context.write(title, result);
        }
    }



    public static void main(String[] args) throws Exception {

        String FILENAME = args[0];

        String input = SeattleBooks.class.getClassLoader().getResource(FILENAME).getFile();

        String output = new String("target/") + args[1];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(SeattleBooks.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
