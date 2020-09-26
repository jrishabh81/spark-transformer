

package com.lwwale.spark.transformer.helpers;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.LinkedList;
import java.util.List;

public class ConvertorHelper {
    public static <T> Seq<T> convertListToSeq(List<T> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    public static Seq<Column> convertListToSeqColumn(Dataset input, LinkedList<String> inputList) {
        List<Column> columnList = new LinkedList<>();
        for (String column : inputList) {
            columnList.add(input.col(column));
        }
        return convertListToSeq(columnList);
    }
}
