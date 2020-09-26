

package com.lwwale.spark.transformer.aggregation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.LinkedList;

public class AggregationHelper {
    private LinkedList<String> groupByColumns;
    private String aggregationType;
    private String aggColumnName;

    public AggregationHelper(LinkedList<String> groupByColumns, String aggregationType, String aggColumnName) {
        this.groupByColumns = groupByColumns;
        this.aggregationType = aggregationType;
        this.aggColumnName = aggColumnName;
    }

    public static Dataset<Row> getAggregatedDataSet(Dataset<Row> input,
                                                    LinkedList<String> groupByColumns,
                                                    String aggregationType,
                                                    String aggColumnName) {
        return AggregationEnum.valueOf(aggregationType.toUpperCase().trim())
                .compute(input, groupByColumns, aggColumnName);
    }


    public Dataset<Row> getAggregatedDataSet(Dataset<Row> input) {
        return AggregationEnum.valueOf(aggregationType.toUpperCase().trim())
                .compute(input, groupByColumns, aggColumnName);
    }
}
