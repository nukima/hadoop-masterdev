import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CountDistinct {
    public static class NumberMapper extends Mapper<Object, Text, Text, NullWritable>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }

    public static class FreqReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{
            context.write(key, NullWritable.get());
        }
    }

    // Count number distinct
    public static class CountMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable oh = new IntWritable(0);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(oh, one);
        }
    }

    public static class CountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        private IntWritable count = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable val : values){
                sum += val.get();
            }
            count.set(sum);
            context.write(key, count);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        //job 1 to list all distinct numbers
        Job job = Job.getInstance(conf, "list distinct");
        job.setJarByClass(CountDistinct.class);
        job.setMapperClass(NumberMapper.class);
        job.setCombinerClass(FreqReducer.class);
        job.setReducerClass(FreqReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/list"));

        job.waitForCompletion(true);

        //job 2 to count number of distinct numbers
        Job job2 = Job.getInstance(conf, "count distinct");
        job2.setJarByClass(CountDistinct.class);
        job2.setMapperClass(CountMapper.class);
        job2.setCombinerClass(CountReducer.class);
        job2.setReducerClass(CountReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1] + "/list"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/result"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
