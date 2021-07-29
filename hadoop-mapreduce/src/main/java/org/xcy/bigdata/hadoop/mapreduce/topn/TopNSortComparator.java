package org.xcy.bigdata.hadoop.mapreduce.topn;

import org.apache.hadoop.io.WritableComparator;

/**
 * 描述：
 *
 * @author ：xuchunyang
 * @date ：7/29/21
 */
public class TopNSortComparator extends WritableComparator {

    public TopNSortComparator() {
        super(TopNKey.class, true);
    }

    @Override
    public int compare(Object o1, Object o2) {
        TopNKey key1 = (TopNKey) o1;
        TopNKey key2 = (TopNKey) o2;

        // 年月正序，温度倒序
        int cmp = Integer.compare(key1.getYear(), key2.getYear());
        if (cmp == 0) {
            cmp = Integer.compare(key1.getMonth(), key2.getMonth());
            if (cmp == 0) {
                return Integer.compare(key2.getTemperator(), key1.getTemperator());
            }
        }
        return cmp;
    }
}
