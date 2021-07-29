package org.xcy.bigdata.hadoop.mapreduce.topn;

import org.apache.hadoop.io.WritableComparator;

/**
 * 描述：
 *
 * @author ：xuchunyang
 * @date ：7/29/21
 */
public class TopNGroupingComparator extends WritableComparator {

    public TopNGroupingComparator() {
        super(TopNKey.class, true);
    }

    @Override
    public int compare(Object o1, Object o2) {
        TopNKey key1 = (TopNKey) o1;
        TopNKey key2 = (TopNKey) o2;

        // 按年月排序
        int cmp = Integer.compare(key1.getYear(), key2.getYear());
        if (cmp == 0) {
            return Integer.compare(key1.getMonth(), key2.getMonth());
        }
        return cmp;
    }
}
