

package com.lwwale.spark.transformer.operands;

public class NumberOperand implements Operand<Double> {
    private Double number;

    public NumberOperand(Double number) {
        this.number = number;
    }

    public Double getNumber() {
        return number;
    }

    public void setNumber(Double number) {
        this.number = number;
    }

    @Override
    public Double get() {
        return getNumber();
    }
}
