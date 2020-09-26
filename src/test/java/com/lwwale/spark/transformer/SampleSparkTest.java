

package com.lwwale.spark.transformer;

import com.lwwale.spark.transformer.aggregation.AggregationEnum;
import com.lwwale.spark.transformer.aggregation.AggregationHelper;
import com.lwwale.spark.transformer.expression.ExpressionEvaluator;
import com.lwwale.spark.transformer.expression.ExpressionUnit;
import com.lwwale.spark.transformer.operands.AggeregatedColumnOperand;
import com.lwwale.spark.transformer.operands.ColumnNameOperand;
import com.lwwale.spark.transformer.operands.NumberOperand;
import com.lwwale.spark.transformer.operations.ArithmaticOperationEnum;
import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;

public class SampleSparkTest extends JavaDatasetSuiteBase {

    private Dataset<Row> input;
    private LinkedList<String> groupByColumns = new LinkedList<>();

    @Override
    public JavaSparkContext jsc() {
        return super.jsc();
    }

    @Before
    public void init() {
        input = sqlContext().read()
                .format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("./src/test/resources/sample_data1.csv");
        groupByColumns.add("well_num");
        groupByColumns.add("rig_id");
    }

    @Test
    public void testAggAVG() {

        Dataset<Row> aggregatedDataSet = AggregationHelper.getAggregatedDataSet(input, groupByColumns,
                "AVG", "rate_of_penetration_ft_per_hr");

        Dataset<Row> output = sqlContext().read()
                .format("com.databricks.spark.csv")
//                .schema(aggregatedDataSet.schema())
                .option("inferSchema", "true")
                .option("header", "true")
                .load("./src/test/resources/sample1_output.csv");

        output.printSchema();
        output.sort().show();
        aggregatedDataSet.printSchema();
        aggregatedDataSet.sort().show();

        ExpressionUnit unit1 = new ExpressionUnit();
        unit1.setOperand1(new NumberOperand(0d));
        unit1.setOperator(ArithmaticOperationEnum.ADD);
        unit1.setOperand2(new AggeregatedColumnOperand(AggregationEnum.AVG, "rate_of_penetration_ft_per_hr", groupByColumns));
        ExpressionEvaluator expressionEvaluator = new ExpressionEvaluator(input);


        Dataset<Row> output1 = expressionEvaluator.evaluate("FAVG_rate_of_penetration_ft_per_hr", unit1);
        output1.show();

        assertDatasetApproximateEquals(aggregatedDataSet.sort(), output.sort(), 0.05);
        assertDatasetApproximateEquals(output1.sort(), output.sort(), 0.05);


    }

    @Test
    public void test() {

//        input.groupBy(groupByColumns.get(0),groupByColumns.subList(1,groupByColumns.size()).toArray(new String[]{})).avg("rate_of_penetration_ft_per_hr").show();


        AggregationHelper.getAggregatedDataSet(input, groupByColumns,
                "AVG", "rate_of_penetration_ft_per_hr").show();


//        standardDeviation(input, "rate_of_penetration_ft_per_hr").show();
//        input.select("well_num").show(10);
//        input.printSchema();

//        Row[] well_nums = (Row[]) input.select("well_num").groupBy().avg().collect();
//        System.out.printf("Well avg : %e", well_nums[0].getDouble(0));
       /* Operator operator= ArithmaticOperationEnum.valueOf("SUBTRACT");
       Dataset<Row> operate = operator.operate(input, "well_num", 1521111470L, "output");
        operate.select("well_num","output").show(10);*/

    }


    ExpressionUnit[] getRule() {
        ExpressionUnit unit1 = new ExpressionUnit();
        unit1.setOperator(ArithmaticOperationEnum.MULTIPLY);
        unit1.setOperand1(new ColumnNameOperand("rate_of_penetration_ft_per_hr"));
        unit1.setOperand2(new NumberOperand(60.0d));

        ExpressionUnit unit2 = new ExpressionUnit();
        unit2.setOperator(ArithmaticOperationEnum.ADD);
        unit2.setOperand1(new ColumnNameOperand("OUTPUT_1"));
        unit2.setOperand2(new NumberOperand(100d));

        ExpressionUnit unit3 = new ExpressionUnit();
        unit3.setOperator(ArithmaticOperationEnum.DIVIDE);
//        unit3.setOperand1(new AggeregatedColumnOperand("", AggregationEnum.AVG, "", (LinkedList<String>) Arrays.asList("", "")));
        unit3.setOperand1(new ColumnNameOperand("OUTPUT_2"));
        unit3.setOperand2(new NumberOperand(100d));
        ExpressionUnit unit4 = new ExpressionUnit();
        unit4.setOperator(ArithmaticOperationEnum.SUBTRACT);
        unit4.setOperand1(new ColumnNameOperand("OUTPUT_3"));
        unit4.setOperand2(new NumberOperand(100d));
        return new ExpressionUnit[]{unit1, unit2, unit3, unit4};
    }

    Dataset<Row> standardDeviation(Dataset<Row> dataset, String columnName) {
        long count = dataset.select(columnName).count();
        System.out.println("Count : " + count);
        Dataset<Row> mean = dataset.select(columnName).groupBy().mean(columnName);
        double meanValue = mean.takeAsList(1).get(0).getDouble(0);


        ExpressionUnit unit = new ExpressionUnit();
        unit.setOperator(ArithmaticOperationEnum.SUBTRACT);
        unit.setOperand1(new ColumnNameOperand(columnName));
        unit.setOperand2(new NumberOperand(meanValue));

        ExpressionUnit unit2 = new ExpressionUnit();
        unit2.setOperator(ArithmaticOperationEnum.MULTIPLY);
        unit2.setOperand1(new ColumnNameOperand("OUTPUT_1"));
        unit2.setOperand2(new ColumnNameOperand("OUTPUT_1"));
        ExpressionEvaluator expressionEvaluator = new ExpressionEvaluator(dataset);
        Dataset<Row> output = expressionEvaluator.evaluate("OUTPUT", unit, unit2);

        output.select("well_num", "rate_of_penetration_ft_per_hr", "OUTPUT").show();
        Dataset<Row> divide = output.groupBy().sum("OUTPUT").withColumnRenamed("sum(OUTPUT)", "SUM_COL").alias("SUM_COL");
        Column as = divide.col("SUM_COL").divide(count).as("STD_DEV");
        return divide.withColumn("STD_DEV", functions.sqrt(as)).select("STD_DEV");

    }


}
