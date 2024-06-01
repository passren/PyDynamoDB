# -*- coding: utf-8 -*-
from pydynamodb.util import strtobool
from pydynamodb.util import flatten_dict
from pydynamodb.sql.util import flatten_list


class TestUtil:
    def test_strtobool(self):
        assert strtobool("true")
        assert strtobool("True")
        assert strtobool("TRUE")
        assert not strtobool("false")
        assert not strtobool("False")
        assert not strtobool("FALSE")

    def test_flatten_dict(self):
        ret = flatten_dict(
            {"a": 1, "c": {"a": 2, "b": {"x": 5, "y": 10}}, "d": [1, 2, 3]}
        )
        assert ret == {
            "a": 1,
            "c.a": 2,
            "c.b.x": 5,
            "c.b.y": 10,
            "d[0]": 1,
            "d[1]": 2,
            "d[2]": 3,
        }

        ret = flatten_dict(
            {
                "a": "a",
                "c": {"a": "b", "b": {"x": 5, "y": 10}},
                "d": [{"a": "a", "b": "b"}, {"a": "c", "b": "d"}],
            }
        )
        assert ret == {
            "a": "a",
            "c.a": "b",
            "c.b.x": 5,
            "c.b.y": 10,
            "d[0].a": "a",
            "d[0].b": "b",
            "d[1].a": "c",
            "d[1].b": "d",
        }

        ret = flatten_dict(
            {
                "a": "a",
                "b": [
                    {
                        "c": "c",
                        "d": [{"a": "a", "b": "b"}, {"a": "c", "b": "d"}],
                        "e": {"a": "a", "b": ["a", "b"]},
                    },
                    {
                        "c": "f",
                        "d": [{"a": "a"}, {"b": "b"}],
                        "e": {"a": "a", "b": ["c", "d"]},
                    },
                ],
            }
        )
        assert ret == {
            "a": "a",
            "b[0].c": "c",
            "b[0].d[0].a": "a",
            "b[0].d[0].b": "b",
            "b[0].d[1].a": "c",
            "b[0].d[1].b": "d",
            "b[0].e.a": "a",
            "b[0].e.b[0]": "a",
            "b[0].e.b[1]": "b",
            "b[1].c": "f",
            "b[1].d[0].a": "a",
            "b[1].d[1].b": "b",
            "b[1].e.a": "a",
            "b[1].e.b[0]": "c",
            "b[1].e.b[1]": "d",
        }

    def test_flatten_list(self):
        ret = flatten_list(
            ["A", "B", ["C", "D", ["E", "F"]], ["G", "H"], 1, [2, 3, [4, 5]]]
        )
        assert ret == ["A", "B", "C", "D", "E", "F", "G", "H", 1, 2, 3, 4, 5]

        ret = flatten_list(
            ["A", "B", ["C", "D", ["[", "E", "F", "]"]], ["G", "H"], 1, [2, 3, [4, 5]]]
        )
        assert ret == ["A", "B", "C", "D", "[E,F]", "G", "H", 1, 2, 3, 4, 5]

        ret = flatten_list(
            ["A", "B", ["C", "D", ["[", 1, 2, "]"]], ["G", "H"], 1, [2, 3, [4, 5]]]
        )
        assert ret == ["A", "B", "C", "D", "[1,2]", "G", "H", 1, 2, 3, 4, 5]
