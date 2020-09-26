

package com.lwwale.spark.transformer.operations;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Operator {
    Dataset<Row> operate(Dataset<Row> df, String column1, String column2, String resultantColumnName);

    Dataset<Row> operate(Dataset<Row> df, String column1, Object number, String resultantColumnName);

    Dataset<Row> operate(Dataset<Row> df, Object number, String column2, String resultantColumnName);

    Dataset<Row> operate(Dataset<Row> df, Column column1, Column column2, String resultantColumnName);

    Dataset<Row> operate(Dataset<Row> df, Column column1, Object number, String resultantColumnName);

    Dataset<Row> operate(Dataset<Row> df, Object number, Column column2, String resultantColumnName);


}
