package tde_2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class YearCategoryUnitWritable implements WritableComparable<YearCategoryUnitWritable> {
    int year;
    String category;
    String unitType;
    public YearCategoryUnitWritable() {};
    public YearCategoryUnitWritable(int year, String category, String unitType) {
        this.year = year;
        this.category = category;
        this.unitType = unitType;
    }

    @Override
    public int compareTo(YearCategoryUnitWritable o) {
        if ((this.year == o.getYear()) &&
                (this.category.compareTo(o.getCategory()) == 0) &&
                (this.unitType.compareTo(o.getUnitType()) == 0)) {
            return 1;
        }
        return 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, category, unitType);
    }



    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.year);
        out.writeUTF(this.category);
        out.writeUTF(this.unitType);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.category = in.readUTF();
        this.unitType = in.readUTF();
    }


    public int getYear() {
        return year;
    }
    public void setYear(int year) {
        this.year = year;
    }

    public String getCategory() {
        return category;
    }
    public void setCategory(String category) {
        this.category = category;
    }

    public String getUnitType() {
        return unitType;
    }
    public void setUnitType(String unitType) {
        this.unitType = unitType;
    }
}
