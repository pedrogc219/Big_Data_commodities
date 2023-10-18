package tde_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

// 1 - (Easy) The number of transactions involving Brazil

public class TransactionsBrazil {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        Job j = new Job(c, "brazilTransactions");

        j.setJarByClass(TransactionsBrazil.class);
        j.setMapperClass(MapTransactionsBrazil.class);
        j.setReducerClass(ReduceTransactionsBrazil.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapTransactionsBrazil extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String linha = value.toString();
            String[] colunas = linha.split(";");
            if (colunas[0].compareTo("Brazil") == 0) {
                con.write(new Text(colunas[0]), new IntWritable(1));
            }
        }
    }
    public static class ReduceTransactionsBrazil extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> value, Context con) throws IOException, InterruptedException {
            int total = 0;
            for(IntWritable v : value) {
                total += v.get();
            }
            con.write(key, new IntWritable(total));
        }
    }
}
