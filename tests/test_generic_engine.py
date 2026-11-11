"""
Unit Tests for Metadata-Driven Data Quality & Rule Validation
"""

import unittest

def evaluate_record_rule(record, dq_rules):
    """Pure Python evaluator simulating PySpark dynamic rule logic for unit testing."""
    for rule in dq_rules:
        col = rule.get("column")
        rule_type = rule.get("rule")

        val = record.get(col)
        if rule_type == "not_null":
            if val is None or val == "":
                return False, f"{col}_IS_NULL"
        elif rule_type == "range":
            min_val = rule.get("min", float("-inf"))
            max_val = rule.get("max", float("inf"))
            if val is None or val < min_val or val > max_val:
                return False, f"{col}_OUT_OF_RANGE"
    return True, None

class TestMetadataEngine(unittest.TestCase):

    def setUp(self):
        self.rules = [
            {"rule": "not_null", "column": "OrderID"},
            {"rule": "range", "column": "TotalAmount", "min": 0}
        ]

    def test_valid_record(self):
        record = {"OrderID": "ORD-101", "TotalAmount": 250.50}
        is_valid, reason = evaluate_record_rule(record, self.rules)
        self.assertTrue(is_valid)
        self.assertIsNone(reason)

    def test_null_pk_rejected(self):
        record = {"OrderID": None, "TotalAmount": 150.00}
        is_valid, reason = evaluate_record_rule(record, self.rules)
        self.assertFalse(is_valid)
        self.assertEqual(reason, "OrderID_IS_NULL")

    def test_negative_amount_rejected(self):
        record = {"OrderID": "ORD-102", "TotalAmount": -50.00}
        is_valid, reason = evaluate_record_rule(record, self.rules)
        self.assertFalse(is_valid)
        self.assertEqual(reason, "TotalAmount_OUT_OF_RANGE")

if __name__ == "__main__":
    unittest.main()
