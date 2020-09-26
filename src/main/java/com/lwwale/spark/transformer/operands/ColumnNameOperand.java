

package com.lwwale.spark.transformer.operands;

public class ColumnNameOperand implements Operand<String> {
    private String column;

    public ColumnNameOperand(String column) {
        this.column = column;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    @Override
    public String get() {
        return getColumn();
    }
}
