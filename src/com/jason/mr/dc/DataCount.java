package com.jason.mr.dc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * <b><code>DataCount</code></b>
 * <p>
 * class_comment
 * <p>
 * <b>Creation Time:</b> 2018/1/3 15:05.
 *
 * @author yangjiangshui
 * @version ${Revision} 2018/1/3
 * @since spring-boot project_version
 */
public class DataCount {
    public static class DCMapper extends Mapper<LongWritable, Text, Text, DataBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            String telNo = fields[0];
            long upPayLoad = Long.parseLong(fields[1]);
            long downPayLoad = Long.parseLong(fields[2]);
            DataBean bean = new DataBean(telNo, upPayLoad, downPayLoad);
            context.write(new Text(telNo), bean);
            /**
             * map{
             * context.write(telNo, bean);
             * }
             */
        }
    }

    public static class DCReducer extends Reducer<Text, DataBean, Text, DataBean> {
        @Override
        protected void reduce(Text key, Iterable<DataBean> v2s, Context context) throws IOException, InterruptedException {
            /**
             * <tel,{bean,bean}
             */
            long up_sum = 0;
            long down_sum = 0;
            for (DataBean bean : v2s) {
                up_sum += bean.getUpPayLoad();
                down_sum += bean.getDownPayLoad();
            }
            DataBean bean = new DataBean("", up_sum, down_sum);
            context.write(key, bean);
        }
    }

    public static class ProviderPartitioner extends Partitioner<Text, DataBean> {
        private static Map<String, Integer> providerMap = new HashMap();

        static {
            providerMap.put("135", 1);
            providerMap.put("137", 1);
            providerMap.put("138", 1);
            providerMap.put("158", 1);
            providerMap.put("188", 2);
        }

        @Override
        public int getPartition(Text key, DataBean dataBean, int i) {
            String account = key.toString();
            String sub_acc = account.substring(0, 3);
            Integer code = providerMap.get(sub_acc);
            if (code == null) {
                code = 0;
            }
            return code;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(DataCount.class);
        job.setMapperClass(DCMapper.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setReducerClass(DCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataBean.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setPartitionerClass(ProviderPartitioner.class);
        job.setNumReduceTasks(Integer.parseInt(args[2]));
        job.waitForCompletion(true);
    }
}
