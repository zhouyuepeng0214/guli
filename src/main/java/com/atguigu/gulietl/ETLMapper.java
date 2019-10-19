package com.atguigu.gulietl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

    private Text k = new Text();
    private StringBuilder sb = new StringBuilder();

    private Counter pass;
    private Counter fail;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        pass = context.getCounter("ETL","Pass");
        fail = context.getCounter("ETL","Fail");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length < 9) {
            fail.increment(1);
        } else {
            fields[3] = fields[3].replace(" ","");
            sb.setLength(0);
            for (int i = 0;i < fields.length;i++) {
                if (i == fields.length - 1) {
                    sb.append(fields[i]);
                } else if (i < 9) {
                    sb.append(fields[i]).append("\t");
                } else {
                    sb.append(fields[i]).append("&");
                }
            }
            k.set(sb.toString());
            context.write(k,NullWritable.get());

            pass.increment(1);
        }
    }
}
