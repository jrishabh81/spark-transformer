

package com.lwwale.spark.transformer.aggregation;

import com.lwwale.spark.transformer.helpers.ConvertorHelper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.JavaConversions;

import java.util.LinkedList;

public enum AggregationEnum {
    MEAN {
        @Override
        public Dataset<Row> compute(Dataset<Row> input, LinkedList<String> groupByColumns, String aggColumn) {
            Dataset<Row> select = input
                    .groupBy(groupByColumns.get(0), groupByColumns.subList(1, groupByColumns.size()).toArray(new String[]{}))
                    .mean(aggColumn);
            select = select.withColumnRenamed("mean(" + aggColumn + ")", "FMEAN_" + aggColumn);
            return input.join(select, JavaConversions.asScalaBuffer(groupByColumns));
        }
    },
    SUM {
        @Override
        public Dataset<Row> compute(Dataset<Row> input, LinkedList<String> groupByColumns, String aggColumn) {
            Dataset<Row> select = input
                    .groupBy(groupByColumns.get(0), groupByColumns.subList(1, groupByColumns.size()).toArray(new String[]{}))
                    .sum(aggColumn);
            select = select.withColumnRenamed("sum(" + aggColumn + ")", "FSUM_" + aggColumn);
            return input.join(select, JavaConversions.asScalaBuffer(groupByColumns));
        }
    },
    MAX {
        @Override
        public Dataset<Row> compute(Dataset<Row> input, LinkedList<String> groupByColumns, String aggColumn) {
            Dataset<Row> select = input
                    .groupBy(ConvertorHelper.convertListToSeqColumn(input, groupByColumns))
                    .max(aggColumn);
            select = select.withColumnRenamed("max(" + aggColumn + ")", "FMAX" + aggColumn);
            return input.join(select, JavaConversions.asScalaBuffer(groupByColumns));
        }
    },
    MIN {
        @Override
        public Dataset<Row> compute(Dataset<Row> input, LinkedList<String> groupByColumns, String aggColumn) {
            Dataset<Row> select = input
                    .groupBy(groupByColumns.get(0), groupByColumns.subList(1, groupByColumns.size()).toArray(new String[]{}))
                    .min(aggColumn);
            select = select.withColumnRenamed("min(" + aggColumn + ")", "FMIN_" + aggColumn);
            return input.join(select, JavaConversions.asScalaBuffer(groupByColumns));
        }
    },
    AVG {
        @Override
        public Dataset<Row> compute(Dataset<Row> input, LinkedList<String> groupByColumns, String aggColumn) {
            Dataset<Row> select = input
                    .groupBy(groupByColumns.get(0), groupByColumns.subList(1, groupByColumns.size()).toArray(new String[]{}))
                    .avg(aggColumn);
            select = select.withColumnRenamed("avg(" + aggColumn + ")", "FAVG_" + aggColumn);
            return input.join(select, JavaConversions.asScalaBuffer(groupByColumns));
        }
    };

    public abstract Dataset<Row> compute(Dataset<Row> input, LinkedList<String> groupByColumns, String aggColumn);
}
