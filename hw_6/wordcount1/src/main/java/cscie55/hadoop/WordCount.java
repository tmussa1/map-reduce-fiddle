package cscie55.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class MyMapper
            extends Mapper<Object, Text, Text, LongWritable>{

        private Text word = new Text();
        private final static LongWritable one = new LongWritable(1L);

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class MyReducer
            extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        // these vars are tmp for dev purposes
        // comment these 3 lines before putting into a jar for running in AWS
//        System.setProperty("hadoop.home.dir", "C:/Users/Tofik Mussa/Desktop/MS in CS/CSCI E-55/Projects/" +
//                "mussa_tofik_homework_6/mussa_tofik_homework_6/hw_6/wordcount1/hadoop-3.0.0");


        String FILENAME = args[0];
        //System.out.println( WordCount.class.getClassLoader().getResource( FILENAME ) );
        String input = WordCount.class.getClassLoader().getResource(FILENAME).getFile();//("").getPath().replaceFirst("^/(.:/)", "$1");
        String output = new String("target/")+args[1]; // replace real cmd line args w/ hard-coded

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // these lines are for dev purposes
        // comment these 2 lines before putting into a for running in AWS
         FileInputFormat.addInputPath(job, new Path("C:/Users/Tofik Mussa/Desktop/MS in CS/CSCI E-55/Projects/" +
                 "mussa_tofik_homework_6/mussa_tofik_homework_6/hw_6/wordcount1/src/main/resources/mobydick.txt"));
         FileOutputFormat.setOutputPath(job, new Path(output));


        // UNCOMMENT these 2 lines before putting into a jar for running in AWS

        //FileInputFormat.addInputPath(job, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


