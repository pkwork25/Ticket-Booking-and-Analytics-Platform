from send_to_kinesis import MAX_RECORDS_PER_PUT, chunk_records, put_with_retries, to_record


def test_partition_key_is_booking_id():
    r = to_record({"event_id": "e1", "booking_id": "BKG-1"})
    assert r["PartitionKey"] == "BKG-1" and b'"event_id":"e1"' in r["Data"]


def test_chunking_respects_kinesis_limits():
    recs = [to_record({"event_id": str(i), "booking_id": f"B{i}"}) for i in range(1203)]
    chunks = chunk_records(recs)
    assert [len(c) for c in chunks] == [500, 500, 203]
    assert max(len(c) for c in chunks) <= MAX_RECORDS_PER_PUT


def test_only_failed_records_are_retried(monkeypatch):
    import send_to_kinesis

    monkeypatch.setattr(send_to_kinesis.time, "sleep", lambda *_: None)
    calls = []

    class FakeKinesis:
        def put_records(self, StreamName, Records):
            calls.append(len(Records))
            if len(calls) == 1:
                return {"FailedRecordCount": 2, "Records": [{"ErrorCode": "ProvisionedThroughputExceededException"}, {"SequenceNumber": "1"}, {"ErrorCode": "x"}]}
            return {"FailedRecordCount": 0, "Records": [{"SequenceNumber": "2"}] * len(Records)}

    recs = [to_record({"event_id": str(i), "booking_id": "B"}) for i in range(3)]
    assert put_with_retries(FakeKinesis(), "s", recs) == 3
    assert calls == [3, 2]
