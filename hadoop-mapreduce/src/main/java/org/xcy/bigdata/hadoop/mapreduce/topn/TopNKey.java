package org.xcy.bigdata.hadoop.mapreduce.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 描述：
 *
 * @author ：xuchunyang
 * @date ：7/29/21
 */
public class TopNKey implements WritableComparable<TopNKey> {

    private int year;
    private int month;
    private int day;
    private int temperator;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getTemperator() {
        return temperator;
    }

    public void setTemperator(int temperator) {
        this.temperator = temperator;
    }

    @Override
    public int compareTo(TopNKey that) {
        int compare = Integer.compare(this.year, that.year);
        if(compare!=0) {
            return compare;
        }
        compare = Integer.compare(this.month, that.month);
        if(compare!=0) {
            return compare;
        }
        compare = Integer.compare(this.day, that.day);
        if(compare!=0) {
            return compare;
        }
        return Integer.compare(that.temperator, this.temperator);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(year);
        out.write(month);
        out.write(day);
        out.write(temperator);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year = in.readInt();
        month = in.readInt();
        day = in.readInt();
        temperator = in.readInt();
    }
}
