

package com.lwwale.spark.transformer.expression;

import com.lwwale.spark.transformer.exception.RuleException;
import com.lwwale.spark.transformer.operands.AggeregatedColumnOperand;
import com.lwwale.spark.transformer.operands.ColumnNameOperand;
import com.lwwale.spark.transformer.operands.NumberOperand;
import com.lwwale.spark.transformer.operands.Operand;
import com.lwwale.spark.transformer.operations.Operator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashSet;
import java.util.Set;

public class ExpressionEvaluator {
    private static final String PREFIX = "OUTPUT_";
    private Dataset<Row> input;
    private Set<String> injectedColumns = new HashSet<>();

    public ExpressionEvaluator(Dataset<Row> input) {
        this.input = input;
    }

    public Dataset<Row> evaluate(String finalOutputName, ExpressionUnit... expressionUnits) {
        Dataset<Row> output = input;
        if (expressionUnits == null || expressionUnits.length < 1) {
            throw new RuleException("No rule to compute");
        }
        for (int i = 0; i < expressionUnits.length; i++) {
            Operator operator = expressionUnits[i].getOperator();
            Operand operand1 = expressionUnits[i].getOperand1();
            Operand operand2 = expressionUnits[i].getOperand2();
            output = execute(output, operand1, operator, operand2, i + 1);
        }
        for (int i = 1; i < expressionUnits.length; i++) {
            output = output.drop(PREFIX.concat("" + i));
        }
        for (String injectedColumn : injectedColumns) {
            output = output.drop(injectedColumn);
        }
        output = output.withColumnRenamed(PREFIX.concat("" + expressionUnits.length), finalOutputName);
        return output;
    }

    private Dataset<Row> execute(Dataset<Row> input, Operand operand1, Operator operator, Operand operand2, Integer sequence) {
        if (operand1 instanceof AggeregatedColumnOperand) {
            input = ((AggeregatedColumnOperand) operand1).injectColumn(input);
            injectedColumns.add(((AggeregatedColumnOperand) operand1).getColumn());
        }
        if (operand2 instanceof AggeregatedColumnOperand) {
            input = ((AggeregatedColumnOperand) operand2).injectColumn(input);
            injectedColumns.add(((AggeregatedColumnOperand) operand2).getColumn());
        }

        if (operand1 instanceof ColumnNameOperand && operand2 instanceof ColumnNameOperand) {
            //column - column
            return operator.operate(input, ((ColumnNameOperand) operand1).get(), ((ColumnNameOperand) operand2).getColumn(), PREFIX.concat(sequence.toString()));
        } else if (operand1 instanceof ColumnNameOperand && operand2 instanceof NumberOperand) {
            // column-number
            return operator.operate(input, ((ColumnNameOperand) operand1).getColumn(), ((NumberOperand) operand2).getNumber(), PREFIX.concat(sequence.toString()));
        } else if (operand1 instanceof NumberOperand && operand2 instanceof ColumnNameOperand) {
            //number-column
            return operator.operate(input, ((NumberOperand) operand1).getNumber(), ((ColumnNameOperand) operand2).getColumn(), PREFIX.concat(sequence.toString()));
        } else
            throw new RuleException("Unsupported expression injected to compute");
    }
}
