package tde_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

// 5 - (Easy) The average price of commodities per unit type, year, and category in the export flow in Brazil

public class AvgPriceBrazil {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();
        Configuration c = new Configuration();

        Job j = new Job(c, "avgPriceBrazil");

        j.setJarByClass(AvgPriceBrazil.class);
        j.setMapperClass(MapAveragePriceBrazil.class);
        j.setReducerClass(ReduceAveragePriceBrazil.class);

        j.setMapOutputKeyClass(LongWritable.class);
        j.setMapOutputValueClass(Text.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapAveragePriceBrazil extends Mapper<LongWritable, Text, YearCategoryUnitWritable, CommodityAvgWritable> {
        public void map(LongWritable key, Text values, Context con) throws IOException, InterruptedException{
            String linha = key.toString();
            String[] colunas = linha.split(";");

            if ((colunas[0].compareTo("Brazil") == 0) && (colunas[4].compareTo("Export") == 0)) {
                YearCategoryUnitWritable yearCategotyUnitKey = new YearCategoryUnitWritable(Integer.parseInt(colunas[1]), colunas[9], colunas[7]);
                CommodityAvgWritable avgPrice = new CommodityAvgWritable(1 , Integer.parseInt(colunas[5]));

                con.write(yearCategotyUnitKey, avgPrice);
            }
        }
    }

    public static class ReduceAveragePriceBrazil extends Reducer<YearCategoryUnitWritable, CommodityAvgWritable, Text, FloatWritable> {
        public void reduce(YearCategoryUnitWritable key, Iterable<CommodityAvgWritable> values, Context con) throws IOException, InterruptedException {
            int nTotal = 0;
            int precototal = 0;
            float media = 0.0f;
            for (CommodityAvgWritable v : values) {
                nTotal += v.getN();
                precototal += v.getPreco();
            }
            con.write(new Text(key.toString()), new FloatWritable(media));
        }
    }
}
