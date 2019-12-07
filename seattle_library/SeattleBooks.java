package cscie55.hadoop;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class SeattleBooks {
    public static class MyMapper
            extends Mapper<Object, Text, SeattleComparator, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text title = new Text();
        private Text count = new Text();
        private SeattleComparator comp = new SeattleComparator();

        public void map(SeattleComparator key, Text value, Context context
        ) throws IOException, InterruptedException {

            CSVParser parser = CSVParser.parse(value.toString(),
                    CSVFormat.RFC4180.withHeader("name", "type", "desc", "yyyy",
                            "MM", "dd", "title", "author", "summary", "short", "year"));

            CSVRecord record = parser.getRecords().get(0);
            title.set(record.get("title"));
            count.set(new Text(String.valueOf(one)));
            comp.set(title, count);
            context.write(comp, one);
        }
    }

    public static class MyReducer
            extends Reducer<SeattleComparator, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private Text title = new Text();

        public void reduce(SeattleComparator key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            title.set(key.title);
            context.write(title, result);
        }
    }

    public static class SeattleComparator implements WritableComparable<SeattleComparator>{


        private Text title;
        private Text count;

        SeattleComparator(){
            set(new Text(), new Text());
        }

        public void set(Text title, Text count){
            this.title = title;
            this.count = count;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            title.write(dataOutput);
            count.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
           title.readFields(dataInput);
           count.readFields(dataInput);
        }

        @Override
        public int compareTo(SeattleComparator seattleComparator) {
            int instanceCount = Integer.parseInt(count.toString());
            int paramCount = Integer.parseInt(seattleComparator.count.toString());

            if(instanceCount == paramCount){
                return 0;
            }

            return instanceCount < paramCount ? -1 : 1;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SeattleComparator that = (SeattleComparator) o;
            return Objects.equals(title, that.title);
        }

        @Override
        public int hashCode() {

            return Objects.hash(title);
        }
    }

    public static void main(String[] args) throws Exception {

        String FILENAME = args[0];

        String input = SeattleBooks.class.getClassLoader().getResource(FILENAME).getFile();

        String output = new String("target/")+args[1];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(SeattleBooks.class);
        job.setMapperClass(SeattleBooks.MyMapper.class);
        job.setCombinerClass(SeattleBooks.MyReducer.class);
        job.setReducerClass(SeattleBooks.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
