#!/usr/bin/env python3
"""Validate a JSON-lines operation history from a reliability run."""

import json
import pathlib
import sys
import unittest
from collections import defaultdict


def checkHistory(path):
    records = []
    required = {"opId", "clientId", "clientSeq", "status", "expectedSize", "observedSize"}
    with pathlib.Path(path).open() as stream:
        for lineNumber, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"line {lineNumber}: invalid JSON: {error}") from error
            missing = required - record.keys()
            if missing:
                raise ValueError(f"line {lineNumber}: missing {sorted(missing)}")
            if record["status"] not in {"OK", "DEGRADED"}:
                raise ValueError(f"line {lineNumber}: unexpected status {record['status']!r}")
            if int(record["observedSize"]) != int(record["expectedSize"]):
                raise ValueError(f"line {lineNumber}: size mismatch for {record['opId']}")
            records.append(record)

    operationIds = [record["opId"] for record in records]
    if len(operationIds) != len(set(operationIds)):
        raise ValueError("duplicate operation id")
    sequences = defaultdict(list)
    for record in records:
        sequences[record["clientId"]].append(int(record["clientSeq"]))
    for clientId, values in sequences.items():
        if values != sorted(values) or len(values) != len(set(values)):
            raise ValueError(f"client sequence is not strictly increasing: {clientId}")
    return len(records)


class HistoryCheckerTest(unittest.TestCase):
    def testAcceptsOrderedHistory(self):
        path = pathlib.Path(self._testMethodName + ".jsonl")
        try:
            path.write_text(
                '{"opId":"a-1","clientId":"a","clientSeq":1,"status":"OK",'
                '"expectedSize":10,"observedSize":10}\n'
                '{"opId":"a-2","clientId":"a","clientSeq":2,"status":"OK",'
                '"expectedSize":20,"observedSize":20}\n'
            )
            self.assertEqual(checkHistory(path), 2)
        finally:
            path.unlink(missing_ok=True)

    def testRejectsDuplicateOperation(self):
        path = pathlib.Path(self._testMethodName + ".jsonl")
        try:
            path.write_text(
                '{"opId":"same","clientId":"a","clientSeq":1,"status":"OK",'
                '"expectedSize":1,"observedSize":1}\n'
                '{"opId":"same","clientId":"a","clientSeq":2,"status":"OK",'
                '"expectedSize":2,"observedSize":2}\n'
            )
            with self.assertRaisesRegex(ValueError, "duplicate"):
                checkHistory(path)
        finally:
            path.unlink(missing_ok=True)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        print(f"validated {checkHistory(sys.argv[1])} operation records")
    else:
        unittest.main()
