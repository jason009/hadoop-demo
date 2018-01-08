package com.jason.mr.sort;

import com.jason.mr.dc.DataBean;
import com.jason.mr.dc.DataCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * <b><code>SumStep</code></b>
 * <p>
 * class_comment
 * <p>
 * <b>Creation Time:</b> 2018/1/4 11:43.
 *
 * @author yangjiangshui
 * @version ${Revision} 2018/1/4
 * @since spring-boot project_version
 */
public class SumStep {
    public static class SumMapper extends Mapper<LongWritable, Text, Text, InfoBean> {
        private InfoBean v = new InfoBean();
        private Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            String account = fields[0];
            double in = Double.parseDouble(fields[1]);
            double out = Double.parseDouble(fields[2]);
            k.set(account);
            v.set(account,in,out);
            context.write(k, v);
        }
    }
    public static class SumReducer extends Reducer<Text, InfoBean, Text, InfoBean> {
        private InfoBean v = new InfoBean();
        @Override
        protected void reduce(Text key, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException {
            double in_sum =0;
            double out_sum =0;
            for (InfoBean bean: values){
                in_sum += bean.getIncome();
                out_sum += bean.getExpenses();
            }
            v.set(key.toString(),in_sum,out_sum);
            context.write(key,v);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(SumStep.class);
        job.setMapperClass(SumMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(InfoBean.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
