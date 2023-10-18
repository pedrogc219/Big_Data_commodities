package tde_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

// 3 - (Easy) The number of transactions per flow type and year

public class TransactionsFlowTypeYear {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        Job j = new Job(c, "transactionsYeartype");

        j.setJarByClass(TransactionsFlowTypeYear.class);
        j.setMapperClass(MapTransactionsYearType.class);
        j.setReducerClass(ReduceTransactionsYearType.class);

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

    public static class MapTransactionsYearType extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String linha = value.toString();
            String[] colunas = linha.split(";");

            String yearType;
            if (colunas[0].compareTo("country_or_area") != 0) {
                yearType = colunas[1]+" "+colunas[4];
                con.write(new Text(yearType), new IntWritable(1));
            }
        }
    }

    public static class ReduceTransactionsYearType extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {

            int total = 0;
            for (IntWritable v : values) {
                total++;
            }
            con.write(new Text(key), new IntWritable(total));
        }
    }
}
