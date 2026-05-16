import unittest

import dsline


class PipelineTests(unittest.TestCase):
    def test_empty_pipeline_repr(self) -> None:
        p = dsline.Pipeline()
        self.assertIn("empty", repr(p))

    # ── source ──

    def test_source_with_numeric_list(self) -> None:
        p = dsline.Pipeline()
        result = p.collect([1, 2, 3])
        self.assertEqual(result, [1, 2, 3])

    def test_source_with_float_list(self) -> None:
        p = dsline.Pipeline()
        result = p.collect([1.5, 2.5])
        self.assertEqual(result, [1.5, 2.5])

    def test_source_with_dict_list(self) -> None:
        p = dsline.Pipeline()
        result = p.collect([{"a": 1}, {"a": 2}])
        self.assertEqual(result, [{"a": 1}, {"a": 2}])

    # ── filter_expr ──

    def test_filter_expr_keeps_matching(self) -> None:
        p = dsline.Pipeline()
        p.filter_expr("x > 2")
        result = p.collect([1, 2, 3, 4, 5])
        self.assertEqual(result, [3, 4, 5])

    def test_filter_expr_with_dict_columns(self) -> None:
        p = dsline.Pipeline()
        p.filter_expr("temp > 20")
        result = p.collect([
            {"temp": 10}, {"temp": 25}, {"temp": 30}
        ])
        self.assertEqual(result, [{"temp": 25}, {"temp": 30}])

    def test_filter_expr_complex_condition(self) -> None:
        p = dsline.Pipeline()
        p.filter_expr("temp > 20 and humidity < 80")
        result = p.collect([
            {"temp": 25, "humidity": 60},
            {"temp": 35, "humidity": 90},
            {"temp": 22, "humidity": 50},
        ])
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["temp"], 25)

    # ── map_expr ──

    def test_map_expr_transforms_values(self) -> None:
        p = dsline.Pipeline()
        p.map_expr("x * 10 + 1")
        result = p.collect([1, 2, 3])
        self.assertEqual(result, [11, 21, 31])

    def test_map_expr_with_dict_columns(self) -> None:
        p = dsline.Pipeline()
        p.map_expr("price * quantity")
        result = p.collect([
            {"price": 10, "quantity": 2},
            {"price": 5, "quantity": 3},
        ])
        self.assertEqual(result, [20, 15])

    # ── chaining ──

    def test_filter_then_map_chain(self) -> None:
        p = dsline.Pipeline()
        p.filter_expr("x > 2")
        p.map_expr("x * 10")
        result = p.collect([1, 2, 3, 4])
        self.assertEqual(result, [30, 40])

    def test_multiple_filters_in_chain(self) -> None:
        p = dsline.Pipeline()
        p.filter_expr("x > 2")
        p.filter_expr("x < 5")
        result = p.collect([1, 2, 3, 4, 5])
        self.assertEqual(result, [3, 4])

    def test_map_then_map_chain(self) -> None:
        p = dsline.Pipeline()
        p.map_expr("x + 1")
        p.map_expr("x * 10")
        result = p.collect([1, 2])
        self.assertEqual(result, [20, 30])

    # ── map_py (Python UDF slow path) ──

    def test_map_py_transforms(self) -> None:
        p = dsline.Pipeline()
        p.map_py(lambda x: x * 2)
        result = p.collect([1, 2, 3])
        self.assertEqual(result, [2, 4, 6])

    def test_map_py_with_dicts(self) -> None:
        p = dsline.Pipeline()
        p.map_py(lambda d: d["val"] + 1)
        result = p.collect([{"val": 1}, {"val": 2}])
        self.assertEqual(result, [2, 3])

    # ── filter_py (Python UDF slow path) ──

    def test_filter_py_drops(self) -> None:
        p = dsline.Pipeline()
        p.filter_py(lambda x: x > 2)
        result = p.collect([1, 2, 3, 4])
        self.assertEqual(result, [3, 4])

    def test_filter_py_with_dicts(self) -> None:
        p = dsline.Pipeline()
        p.filter_py(lambda d: d["score"] >= 80)
        result = p.collect([
            {"score": 75}, {"score": 80}, {"score": 95}
        ])
        self.assertEqual(result, [{"score": 80}, {"score": 95}])

    # ── mixed Rust / Python chains ──

    def test_filter_expr_then_map_py(self) -> None:
        p = dsline.Pipeline()
        p.filter_expr("x > 2")
        p.map_py(lambda x: x * 10)
        result = p.collect([1, 2, 3, 4])
        self.assertEqual(result, [30, 40])

    def test_map_expr_then_filter_py(self) -> None:
        p = dsline.Pipeline()
        p.map_expr("x * 10")
        p.filter_py(lambda x: x > 20)
        result = p.collect([1, 2, 3])
        self.assertEqual(result, [30])

    # ── edge cases ──

    def test_filter_expr_all_dropped(self) -> None:
        p = dsline.Pipeline()
        p.filter_expr("x > 100")
        result = p.collect([1, 2, 3])
        self.assertEqual(result, [])

    def test_empty_source(self) -> None:
        p = dsline.Pipeline()
        result = p.collect([])
        self.assertEqual(result, [])

    def test_filter_expr_missing_column_treated_as_false(self) -> None:
        p = dsline.Pipeline()
        p.filter_expr("nonexistent > 0")
        result = p.collect([1, 2, 3])
        self.assertEqual(result, [])

    def test_map_expr_missing_column_yields_nan(self) -> None:
        import math
        p = dsline.Pipeline()
        p.map_expr("nonexistent")
        result = p.collect([1])
        self.assertEqual(len(result), 1)
        self.assertTrue(math.isnan(result[0]))

    def test_rejects_larger_pipeline(self) -> None:
        p = dsline.Pipeline()
        p.filter_expr("x > 2")
        p.map_expr("x * x")
        p.filter_expr("x > 10")
        p.map_expr("x - 0.5")
        result = p.collect([1, 2, 3, 4, 5, 6])
        self.assertEqual(result, [15.5, 24.5, 35.5])

    # ── errors ──

    def test_invalid_filter_expression_raises(self) -> None:
        p = dsline.Pipeline()
        with self.assertRaises(dsline.PipelineBuildError):
            p.filter_expr("x @ 2")

    def test_invalid_map_expression_raises(self) -> None:
        p = dsline.Pipeline()
        with self.assertRaises(dsline.PipelineBuildError):
            p.map_expr("x +")

    def test_non_numeric_source_raises(self) -> None:
        p = dsline.Pipeline()
        with self.assertRaises(ValueError):
            p.collect(["hello"])

    def test_non_numeric_dict_values_skipped(self) -> None:
        p = dsline.Pipeline()
        # Non-numeric values are silently skipped; only numeric columns
        # participate in expr-lite evaluation.
        result = p.collect([{"name": "alice", "score": 90.0}])
        self.assertEqual(result, [{"score": 90.0}])

    def test_dict_with_no_numeric_values_becomes_empty(self) -> None:
        p = dsline.Pipeline()
        result = p.collect([{"key": "value"}])
        self.assertEqual(result, [{}])

    def test_filter_py_raises_propagates(self) -> None:
        p = dsline.Pipeline()
        p.filter_py(lambda x: 1 / 0)  # will raise
        with self.assertRaises(ZeroDivisionError):
            p.collect([1])

    def test_map_py_raises_propagates(self) -> None:
        p = dsline.Pipeline()
        p.map_py(lambda x: 1 / 0)
        with self.assertRaises(ZeroDivisionError):
            p.collect([1])

    # ── batch ──

    def test_batch_groups_items(self) -> None:
        p = dsline.Pipeline()
        p.batch(2)
        result = p.collect([1, 2, 3, 4])
        self.assertEqual(result, [[1, 2], [3, 4]])

    def test_batch_trailing_partial_emitted(self) -> None:
        p = dsline.Pipeline()
        p.batch(3)
        result = p.collect([1, 2, 3, 4, 5])
        self.assertEqual(result, [[1, 2, 3], [4, 5]])

    def test_batch_size_1(self) -> None:
        p = dsline.Pipeline()
        p.batch(1)
        result = p.collect([1, 2, 3])
        self.assertEqual(result, [[1], [2], [3]])

    def test_batch_then_filter_py(self) -> None:
        p = dsline.Pipeline()
        p.batch(2)
        p.filter_py(lambda batch: sum(batch) > 5)
        result = p.collect([1, 5, 2, 6])
        self.assertEqual(result, [[1, 5], [2, 6]])

    def test_batch_then_map_py(self) -> None:
        p = dsline.Pipeline()
        p.batch(3)
        p.map_py(lambda batch: sum(batch))
        result = p.collect([1, 2, 3, 4, 5, 6])
        self.assertEqual(result, [6, 15])

    def test_filter_expr_before_batch(self) -> None:
        p = dsline.Pipeline()
        p.filter_expr("x > 2")
        p.batch(2)
        result = p.collect([1, 2, 3, 4, 5])
        self.assertEqual(result, [[3, 4], [5]])

    def test_batch_size_zero_raises(self) -> None:
        p = dsline.Pipeline()
        with self.assertRaises(ValueError):
            p.batch(0)

    # ── select ──

    def test_select_keeps_only_named_columns(self) -> None:
        p = dsline.Pipeline()
        p.select(["a", "c"])
        result = p.collect([{"a": 1, "b": 2, "c": 3}])
        self.assertEqual(result, [{"a": 1, "c": 3}])

    def test_select_pass_through_scalars(self) -> None:
        p = dsline.Pipeline()
        p.select(["a"])
        result = p.collect([1, 2])
        self.assertEqual(result, [1, 2])

    def test_select_then_filter_expr(self) -> None:
        p = dsline.Pipeline()
        p.select(["score"])
        p.filter_expr("score > 80")
        result = p.collect([{"name": "a", "score": 90}, {"name": "b", "score": 70}])
        self.assertEqual(result, [{"score": 90}])

    # ── map_py_batch / filter_py_batch ──

    def test_map_py_batch_operates_per_element(self) -> None:
        p = dsline.Pipeline()
        p.batch(3)
        p.map_py_batch(lambda x: x * 2)
        result = p.collect([1, 2, 3, 4, 5, 6])
        self.assertEqual(result, [[2, 4, 6], [8, 10, 12]])

    def test_filter_py_batch_removes_elements(self) -> None:
        p = dsline.Pipeline()
        p.batch(4)
        p.filter_py_batch(lambda x: x > 3)
        result = p.collect([1, 2, 3, 4])
        self.assertEqual(result, [[4]])

    def test_map_then_filter_py_batch_chain(self) -> None:
        p = dsline.Pipeline()
        p.batch(3)
        p.map_py_batch(lambda x: x * 10)
        p.filter_py_batch(lambda x: x > 15)
        result = p.collect([1, 2, 3, 4, 5, 6])
        self.assertEqual(result, [[20, 30], [40, 50, 60]])

    def test_map_py_batch_without_prior_batch(self) -> None:
        # Falls back to per-item behavior.
        p = dsline.Pipeline()
        p.map_py_batch(lambda x: x * 2)
        result = p.collect([1, 2, 3])
        self.assertEqual(result, [2, 4, 6])

    def test_filter_py_batch_without_prior_batch(self) -> None:
        p = dsline.Pipeline()
        p.filter_py_batch(lambda x: x > 2)
        result = p.collect([1, 2, 3])
        self.assertEqual(result, [3])

    def test_batch_map_py_batch_then_regular_map_py(self) -> None:
        p = dsline.Pipeline()
        p.batch(2)
        p.map_py_batch(lambda x: x + 1)
        p.map_py(lambda batch: sum(batch))
        result = p.collect([1, 2, 3, 4])
        self.assertEqual(result, [5, 9])  # (1+1)+(2+1)=5, (3+1)+(4+1)=9

    def test_map_py_batch_on_dicts(self) -> None:
        p = dsline.Pipeline()
        p.batch(2)
        p.map_py_batch(lambda d: d["val"] * 2)
        result = p.collect([{"val": 1}, {"val": 2}, {"val": 3}])
        self.assertEqual(result, [[2, 4], [6]])

    def test_filter_py_batch_raises_propagates(self) -> None:
        p = dsline.Pipeline()
        p.batch(2)
        p.filter_py_batch(lambda x: 1 / 0)
        with self.assertRaises(ZeroDivisionError):
            p.collect([1, 2])


if __name__ == "__main__":
    unittest.main()
