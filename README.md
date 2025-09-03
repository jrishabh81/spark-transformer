# KPI Evaluator - spark-transformer

## Overview

`spark-transformer` is a utility library designed to simplify mathematical and KPI (Key Performance Indicator) computations on Apache Spark DataFrames. It provides a flexible, expression-based API to perform arithmetic operations, aggregations, and custom column transformations in a declarative and reusable manner.

## Key Features

- **Expression-based Computation:** Build complex mathematical expressions using a chain of operations and operands.
- **Aggregation Support:** Easily compute aggregates (AVG, SUM, etc.) grouped by specified columns.
- **Custom Column Injection:** Inject computed or aggregated columns into your DataFrame pipeline.
- **Extensible Operators:** Supports addition, subtraction, multiplication, division, and can be extended for more.
- **Integration with Spark SQL:** Leverages Spark SQL's performance and scalability.

## Benefits

- **Reduces Boilerplate:** No need to manually write repetitive Spark SQL code for KPI calculations.
- **Reusable Logic:** Define and reuse expression units for different datasets and KPIs.
- **Testable:** Comes with test utilities and sample tests for validation.
- **Declarative:** Focus on *what* to compute, not *how* to compute it.

## Usage Example

```java
// Prepare your input DataFrame and groupBy columns
Dataset<Row> input = ...;
List<String> groupByColumns = Arrays.asList("well_num", "rig_id");

// Define an expression: (AVG(rate_of_penetration_ft_per_hr) + 0)
ExpressionUnit unit = new ExpressionUnit();
unit.setOperand1(new NumberOperand(0d));
unit.setOperator(ArithmaticOperationEnum.ADD);
unit.setOperand2(new AggeregatedColumnOperand(AggregationEnum.AVG, "rate_of_penetration_ft_per_hr", groupByColumns));

// Evaluate the expression
ExpressionEvaluator evaluator = new ExpressionEvaluator(input);
Dataset<Row> result = evaluator.evaluate("FAVG_rate_of_penetration_ft_per_hr", unit);
result.show();
```

## Getting Started

1. Add the library to your Maven project (see `pom.xml` for dependencies).
2. Use the provided operands, operators, and evaluators to build and execute your KPI logic.
3. See [`SampleSparkTest.java`](https://github.com/jrishabh81/spark-transformer/blob/master/src/test/java/com/lwwale/spark/transformer/SampleSparkTest.java) for more usage examples.

---

This library is ideal for data engineers and analysts working with Spark who need a clean, maintainable way to compute KPIs and mathematical expressions on large datasets.
