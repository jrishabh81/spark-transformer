

package com.lwwale.spark.transformer.operations;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public enum ArithmaticOperationEnum implements Operator {
    ADD {
        @Override
        public Dataset<Row> operate(Dataset<Row> df, String column1, String column2, String resultantColumnName) {
            Column resultantColumn = df.col(column1).plus(df.col(column2)).as(resultantColumnName)/*.alias()*/;
            return df.withColumn(resultantColumnName, resultantColumn);
        }

        @Override
        public Dataset<Row> operate(Dataset<Row> df, String column1, Object number, String resultantColumnName) {
            Column resultantColumn = df.col(column1).plus(number).as(resultantColumnName);
            return df.withColumn(resultantColumnName, resultantColumn);
        }

        @Override
        public Dataset<Row> operate(Dataset<Row> df, Object number, String column, String resultantColumnName) {
            return operate(df, column, number, resultantColumnName);
        }

        @Override
        public Dataset<Row> operate(Dataset<Row> df, Column column1, Column column2, String resultantColumnName) {
            return df.withColumn(resultantColumnName, column1.plus(column2).as(resultantColumnName));
        }

        @Override
        public Dataset<Row> operate(Dataset<Row> df, Column column, Object number, String resultantColumnName) {
            return df.withColumn(resultantColumnName, column.plus(number).as(resultantColumnName));
        }

        @Override
        public Dataset<Row> operate(Dataset<Row> df, Object number, Column column, String resultantColumnName) {
            return operate(df, column, number, resultantColumnName);
        }
    },
    SUBTRACT {
        @Override
        public Dataset<Row> operate(Dataset<Row> df, String column1, String column2, String resultantColumnName) {
            Column resultantColumn = df.col(column1).minus(df.col(column2)).as(resultantColumnName);
            return df.withColumn(resultantColumnName, resultantColumn);
        }

        @Override
        public Dataset<Row> operate(Dataset<Row> df, String column1, Object number, String resultantColumnName) {
            Column resultantColumn = df.col(column1).minus(number).as(resultantColumnName);
            return df.withColumn(resultantColumnName, resultantColumn);
        }

        @Override
        public Dataset<Row> operate(Dataset<Row> df, Object number, String column, String resultantColumnName) {
            return df.withColumn(resultantColumnName, functions.lit(number).minus(df.col(column).as(resultantColumnName)));
        }

        @Override
        public Dataset<Row> operate(Dataset<Row> df, Column column1, Column column2, String resultantColumnName) {
            return df.withColumn(resultantColumnName, column1.minus(column2).as(resultantColumnName));
        }

        @Override
        public Dataset<Row> operate(Dataset<Row> df, Column column1, Object number, String resultantColumnName) {
            return df.withColumn(resultantColumnName, column1.minus(number).as(resultantColumnName));
        }

        @Override
        public Dataset<Row> operate(Dataset<Row> df, Object number, Column column2, String resultantColumnName) {
            return df.withColumn(resultantColumnName, functions.lit(number).minus(column2).as(resultantColumnName));
        }
    },
    MULTIPLY {
        @Override
        public Dataset<Row> operate(Dataset<Row> df, String column1, String column2, String resultantColumnName) {
            Column resultantColumn = df.col(column1).multiply(df.col(column2)).as(resultantColumnName);
            return df.withColumn(resultantColumnName, resultantColumn);
        }

        @Override
        public Dataset<Row> operate(Dataset<Row> df, String column1, Object number, String resultantColumnName) {
            Column resultantColumn = df.col(column1).multiply(number).as(resultantColumnName);
            return df.withColumn(resultantColumnName, resultantColumn);
        }

        @Override
        public Dataset<Row> operate(Dataset<Row> df, Object number, String column, String resultantColumnName) {
            return operate(df, column, number, resultantColumnName);
        }

        @Override
        public Dataset<Row> operate(Dataset<Row> df, Column column1, Column column2, String resultantColumnName) {
            return df.withColumn(resultantColumnName, column1.multiply(column2).as(resultantColumnName));
        }

        @Override
        public Dataset<Row> operate(Dataset<Row> df, Column column1, Object number, String resultantColumnName) {
            return df.withColumn(resultantColumnName, column1.multiply(number).as(resultantColumnName));
        }

        @Override
        public Dataset<Row> operate(Dataset<Row> df, Object number, Column column2, String resultantColumnName) {
            return operate(df, column2, number, resultantColumnName);
        }
    },
    DIVIDE {
        public Dataset<Row> operate(Dataset<Row> df, String column1, String column2, String resultantColumnName) {
            Column resultantColumn = df.col(column1).divide(df.col(column2)).as(resultantColumnName);
            return df.withColumn(resultantColumnName, resultantColumn);
        }

        public Dataset<Row> operate(Dataset<Row> df, String column1, Object number, String resultantColumnName) {
            Column resultantColumn = df.col(column1).divide(number).as(resultantColumnName);
            return df.withColumn(resultantColumnName, resultantColumn);
        }

        public Dataset<Row> operate(Dataset<Row> df, Object number, String column, String resultantColumnName) {
            Column resultantColumn = functions.lit(number).divide(df.col(column)).as(resultantColumnName);
            return df.withColumn(resultantColumnName, resultantColumn);
        }

        @Override
        public Dataset<Row> operate(Dataset<Row> df, Column column1, Column column2, String resultantColumnName) {
            return df.withColumn(resultantColumnName, column1.divide(column2).as(resultantColumnName));
        }

        @Override
        public Dataset<Row> operate(Dataset<Row> df, Column column1, Object number, String resultantColumnName) {
            return df.withColumn(resultantColumnName, column1.divide(number).as(resultantColumnName));
        }

        @Override
        public Dataset<Row> operate(Dataset<Row> df, Object number, Column column2, String resultantColumnName) {
            return df.withColumn(resultantColumnName, functions.lit(number).divide(column2).as(resultantColumnName));
        }
    }

}
