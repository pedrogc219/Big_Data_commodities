package tde_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

// 4 - (Easy) The average of commodity values per year

public class AvgCommodityValueYear {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();
        Configuration c = new Configuration();

        Job j = new Job(c, "averageValueYear");

        j.setJarByClass(AvgCommodityValueYear.class);
        j.setMapperClass(MapAverageCommodityValueYear.class);
        j.setReducerClass(ReduceAverageCommodityValueYear.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(CommodityAvgWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapAverageCommodityValueYear extends Mapper<LongWritable, Text, Text, CommodityAvgWritable> {
        public void map(LongWritable key, Text values, Context con) throws IOException, InterruptedException {
            String linha = values.toString();
            String[] colunas = linha.split(";");

            if (colunas[0].compareTo("country_or_area") != 0) {
                CommodityAvgWritable comAvg = new CommodityAvgWritable(1, Float.parseFloat(colunas[5]));
                con.write(new Text(colunas[1]), comAvg);
            }
        }
    }

    public static class ReduceAverageCommodityValueYear extends Reducer<Text, CommodityAvgWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<CommodityAvgWritable> values, Context con) throws IOException, InterruptedException {
            int nTotal = 0;
            float precoTotal = 0.0f;
            float media = 0.0f;
            for (CommodityAvgWritable v : values) {
                nTotal += v.getN();
                precoTotal += v.getPreco();
            }
            media = precoTotal/nTotal;
            con.write(key, new FloatWritable(media));
        }
    }
}
