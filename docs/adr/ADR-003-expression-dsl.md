# ADR-003: Expression DSL

Status: draft

## Decision

0.1 uses expr-lite for native operators. Core dsline will not depend on DataFusion.

The expr-lite grammar includes numeric literals, column references, arithmetic, comparisons, boolean operators, and parentheses. It excludes SQL, joins, aggregation, string functions, embedded Python UDFs, and DataFusion expressions.

## Consequences

Operator implementation stays small enough for the MVP. Arrow compute kernels and DataFusion can be evaluated later behind optional features.
