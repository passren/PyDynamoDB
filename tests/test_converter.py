# -*- coding: utf-8 -*-
from collections import Counter
from datetime import datetime, date


class TestParameterConverter:
    def test_serializer_str(self, converter):
        assert converter.serialize("string") == {"S": "string"}

    def test_serializer_number(self, converter):
        assert converter.serialize(2) == {"N": "2"}
        assert converter.serialize(2.3) == {"N": "2.3"}
        assert converter.serialize(2.3563) == {"N": "2.3563"}
        assert converter.serialize(2.0) == {"N": "2.0"}

    def test_serializer_binary(self, converter):
        assert converter.serialize(b"XYZ") == {"B": "XYZ"}
        assert converter.serialize(b"123") == {"B": "123"}

    def test_serializer_strset(self, converter):
        expected_ = {"SS": ["A", "B", "C"]}
        result_ = converter.serialize({"A", "B", "C"})
        assert result_.get("SS", None) is not None
        assert dict(Counter(result_["SS"])) == dict(Counter(expected_["SS"]))

    def test_serializer_numberset(self, converter):
        expected_ = {"NS": ["1", "2", "3.2456", "4.0"]}
        result_ = converter.serialize({1, 2, 3.2456, 4.0})
        assert result_.get("NS", None) is not None
        assert dict(Counter(result_["NS"])) == dict(Counter(expected_["NS"]))

    def test_serializer_binset(self, converter):
        expected_ = {"BS": ["XYZ", "123", "#$@"]}
        result_ = converter.serialize({b"XYZ", b"123", b"#$@"})
        assert result_.get("BS", None) is not None
        assert dict(Counter(result_["BS"])) == dict(Counter(expected_["BS"]))

    def test_serializer_list(self, converter):
        assert converter.serialize(["A", 1, 2.356, b"XYZ"]) == {
            "L": [{"S": "A"}, {"N": "1"}, {"N": "2.356"}, {"B": "XYZ"}]
        }
        assert converter.serialize(
            ["A", ["A", 1, 2.356, b"XYZ"], {"A": "1", "B": 2, "C": b"XYZ"}]
        ) == {
            "L": [
                {"S": "A"},
                {"L": [{"S": "A"}, {"N": "1"}, {"N": "2.356"}, {"B": "XYZ"}]},
                {"M": {"A": {"S": "1"}, "B": {"N": "2"}, "C": {"B": "XYZ"}}},
            ]
        }

    def test_serializer_map(self, converter):
        assert converter.serialize({"A": "1", "B": 2, "C": b"XYZ"}) == {
            "M": {"A": {"S": "1"}, "B": {"N": "2"}, "C": {"B": "XYZ"}}
        }

        assert converter.serialize(
            {
                "A": "1",
                "B": 2,
                "C": b"XYZ",
                "List": ["A", 1, 2.356, b"XYZ"],
                "Map": {"A": "1", "B": 2, "C": b"XYZ"},
            }
        ) == {
            "M": {
                "A": {"S": "1"},
                "B": {"N": "2"},
                "C": {"B": "XYZ"},
                "List": {"L": [{"S": "A"}, {"N": "1"}, {"N": "2.356"}, {"B": "XYZ"}]},
                "Map": {"M": {"A": {"S": "1"}, "B": {"N": "2"}, "C": {"B": "XYZ"}}},
            }
        }

    def test_serializer_null(self, converter):
        assert converter.serialize(None) == {"NULL": True}

    def test_serializer_bool(self, converter):
        assert converter.serialize(True) == {"BOOL": True}
        assert converter.serialize(False) == {"BOOL": False}

    def test_serializer_datetime(self, converter):
        assert converter.serialize(datetime(2022, 9, 10)) == {
            "S": "2022-09-10T00:00:00"
        }
        assert converter.serialize(datetime(2022, 9, 10, 14, 23, 8)) == {
            "S": "2022-09-10T14:23:08"
        }
        assert converter.serialize(date(2022, 9, 10)) == {"S": "2022-09-10"}


class TestResponseConverter:
    def test_deserializer_str(self, converter):
        assert converter.deserialize({"S": "string"}) == "string"

    def test_deserializer_datetime(self, converter):
        assert converter.deserialize(
            {"S": "2022-09-10T00:00:00.000"}, function="DATETIME"
        ) == datetime(2022, 9, 10)
        assert converter.deserialize(
            {"S": "2022-09-10"},
            function="DATETIME",
        ) == datetime(2022, 9, 10)
        assert converter.deserialize(
            {"S": "2022/09/10"}, function="DATE", function_params=["%Y/%m/%d"]
        ) == date(2022, 9, 10)

    def test_deserializer_substr(self, converter):
        assert (
            converter.deserialize(
                {"S": "abcdefg01234"}, function="SUBSTR", function_params=[6]
            )
            == "g01234"
        )

        assert (
            converter.deserialize(
                {"S": "abcdefg01234"}, function="SUBSTR", function_params=[1, 3]
            )
            == "bcd"
        )

        assert (
            converter.deserialize(
                {"S": "abcdefg01234"}, function="SUBSTRING", function_params=[1, 3]
            )
            == "bcd"
        )

        assert (
            converter.deserialize(
                {"S": "abcdefg01234"}, function="SUBSTRING", function_params=[1, 100]
            )
            == "bcdefg01234"
        )

    def test_deserializer_trim(self, converter):
        assert (
            converter.deserialize({"S": "  Hello World   "}, function="TRIM")
            == "Hello World"
        )

    def test_deserializer_ltrim(self, converter):
        assert (
            converter.deserialize({"S": "  Hello World   "}, function="LTRIM")
            == "Hello World   "
        )

    def test_deserializer_rtrim(self, converter):
        assert (
            converter.deserialize({"S": "  Hello World   "}, function="RTRIM")
            == "  Hello World"
        )

    def test_deserializer_upper(self, converter):
        assert converter.deserialize({"S": "abcdEFG"}, function="UPPER") == "ABCDEFG"

    def test_deserializer_lower(self, converter):
        assert converter.deserialize({"S": "abcdEFG"}, function="LOWER") == "abcdefg"

    def test_deserializer_replace(self, converter):
        assert (
            converter.deserialize(
                {"S": "abcdefgg_-001234"}, function="REPLACE", function_params=["g"]
            )
            == "abcdef_-001234"
        )

        assert (
            converter.deserialize(
                {"S": "abcdefgg_-001234"},
                function="REPLACE",
                function_params=["g", "X"],
            )
            == "abcdefXX_-001234"
        )

    def test_deserializer_number(self, converter):
        assert converter.deserialize({"N": "2"}) == 2
        assert converter.deserialize({"N": "2.3"}) == float(2.3)
        assert converter.deserialize({"N": "2.3563"}) == float(2.3563)
        assert converter.deserialize({"N": "2.0"}) == float(2.0)

    def test_deserializer_binary(self, converter):
        assert converter.deserialize({"B": b"XYZ"}) == b"XYZ"
        assert converter.deserialize({"B": b"123"}) == b"123"

    def test_deserializer_strset(self, converter):
        expected_ = {"A", "B", "C"}
        result_ = converter.deserialize({"SS": ["A", "B", "C"]})
        assert dict(Counter(result_)) == dict(Counter(expected_))

    def test_deserializer_numberset(self, converter):
        expected_ = {1, 2, 3.2456, 4.0}
        result_ = converter.deserialize({"NS": ["1", "2", "3.2456", "4.0"]})
        assert dict(Counter(result_)) == dict(Counter(expected_))

    def test_deserializer_binset(self, converter):
        expected_ = {b"XYZ", b"123", b"#$@"}
        result_ = converter.deserialize({"BS": [b"XYZ", b"123", b"#$@"]})
        assert dict(Counter(result_)) == dict(Counter(expected_))

    def test_deserializer_list(self, converter):
        assert converter.deserialize(
            {"L": [{"S": "A"}, {"N": "1"}, {"N": "2.356"}, {"B": b"XYZ"}]}
        ) == ["A", 1, 2.356, b"XYZ"]

        assert converter.deserialize(
            {
                "L": [
                    {"S": "A"},
                    {"L": [{"S": "A"}, {"N": "1"}, {"N": "2.356"}, {"B": b"XYZ"}]},
                    {"M": {"A": {"S": "1"}, "B": {"N": "2"}, "C": {"B": b"XYZ"}}},
                ]
            }
        ) == ["A", ["A", 1, 2.356, b"XYZ"], {"A": "1", "B": 2, "C": b"XYZ"}]

    def test_deserializer_map(self, converter):
        assert converter.deserialize(
            {"M": {"A": {"S": "1"}, "B": {"N": "2"}, "C": {"B": b"XYZ"}}}
        ) == {"A": "1", "B": 2, "C": b"XYZ"}

        assert converter.deserialize(
            {
                "M": {
                    "A": {"S": "1"},
                    "B": {"N": "2"},
                    "C": {"B": b"XYZ"},
                    "List": {
                        "L": [{"S": "A"}, {"N": "1"}, {"N": "2.356"}, {"B": b"XYZ"}]
                    },
                    "Map": {
                        "M": {"A": {"S": "1"}, "B": {"N": "2"}, "C": {"B": b"XYZ"}}
                    },
                }
            }
        ) == {
            "A": "1",
            "B": 2,
            "C": b"XYZ",
            "List": ["A", 1, 2.356, b"XYZ"],
            "Map": {"A": "1", "B": 2, "C": b"XYZ"},
        }

    def test_deserializer_null(self, converter):
        assert converter.deserialize({"NULL": True}) is None
        assert converter.deserialize({"NULL": False}) is None

    def test_deserializer_bool(self, converter):
        assert converter.deserialize({"BOOL": True})
        assert not converter.deserialize({"BOOL": False})
