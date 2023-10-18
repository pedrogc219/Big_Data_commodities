package tde_2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 4 - (Easy) The average of commodity values per year
// 5 - (Easy) The average price of commodities per unit type, year, and category in the export flow in Brazil


public class CommodityAvgWritable implements Writable {
    int n;
    float preco;
    public CommodityAvgWritable() {};
    public CommodityAvgWritable(int n, float value) {
        this.n = n;
        this.preco = value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(n);
        out.writeFloat(preco);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.n = in.readInt();
        this.preco = in.readInt();
    }

    public int getN() {
        return n;
    }
    public void setN(int n) {
        this.n = n;
    }

    public float getPreco() {
        return preco;
    }
    public void setPreco(int value) {
        this.preco = value;
    }
}
