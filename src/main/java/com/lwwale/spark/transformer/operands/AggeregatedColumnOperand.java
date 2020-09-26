

package com.lwwale.spark.transformer.operands;

import com.lwwale.spark.transformer.aggregation.AggregationEnum;
import com.lwwale.spark.transformer.aggregation.AggregationHelper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.LinkedList;

public class AggeregatedColumnOperand extends ColumnNameOperand {
    private AggregationEnum typeOfAggregation;
    private String aggColumnName;
    private LinkedList<String> groupingColumns;

    public AggeregatedColumnOperand(AggregationEnum typeOfAggregation, String aggColumnName, LinkedList<String> groupingColumns) {
        super("F" + (typeOfAggregation.name()) + "_" + aggColumnName);
        this.typeOfAggregation = typeOfAggregation;
        this.aggColumnName = aggColumnName;
        this.groupingColumns = groupingColumns;
    }

    public Dataset<Row> injectColumn(Dataset<Row> inpuDataset) {
        return AggregationHelper.getAggregatedDataSet(
                inpuDataset,
                this.groupingColumns,
                this.typeOfAggregation.name(),
                aggColumnName);
    }
}
