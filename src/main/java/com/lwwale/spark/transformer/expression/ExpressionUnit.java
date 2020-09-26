

package com.lwwale.spark.transformer.expression;

import com.lwwale.spark.transformer.operands.Operand;
import com.lwwale.spark.transformer.operations.Operator;

import java.util.Objects;

public class ExpressionUnit {
    private Operand operand1;
    private Operator operator;
    private Operand operand2;

    public Operand getOperand1() {
        return operand1;
    }

    public void setOperand1(Operand operand1) {
        this.operand1 = operand1;
    }

    public Operator getOperator() {
        return operator;
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    public Operand getOperand2() {
        return operand2;
    }

    public void setOperand2(Operand operand2) {
        this.operand2 = operand2;
    }

   /* public int getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(int sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }*/

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExpressionUnit)) return false;
        ExpressionUnit that = (ExpressionUnit) o;
        return Objects.equals(getOperand1(), that.getOperand1()) &&
                Objects.equals(getOperator(), that.getOperator()) &&
                Objects.equals(getOperand2(), that.getOperand2());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getOperand1(), getOperator(), getOperand2());
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
