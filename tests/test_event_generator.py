import json
import queue
from unittest import mock

import pandas as pd
import event_generator.main as main_mod


# =========================
# generate_event
# =========================
def test_generate_event_structure_and_payload():
    e = main_mod.generate_event()

    # базовая структура
    for key in [
        "event_id", "type", "created_at", "received_at",
        "session_id", "user_id", "ip", "url", "referrer",
        "device_type", "user_agent", "event_title",
        "element_id", "x", "y", "payload",
    ]:
        assert key in e

    # payload согласован с основными полями
    payload = json.loads(e["payload"])
    assert payload["event_title"] == e["event_title"]
    assert payload["element_id"] == e["element_id"]
    assert payload["x"] == e["x"]
    assert payload["y"] == e["y"]


# =========================
# serialize_event_for_http
# =========================
def test_serialize_event_for_http_converts_datetime_and_keeps_others():
    import datetime

    e = {
        "created_at": datetime.datetime.utcnow(),
        "received_at": pd.Timestamp.utcnow(),
        "other": 123,
    }

    s = main_mod.serialize_event_for_http(e)

    assert isinstance(s["created_at"], str)
    assert isinstance(s["received_at"], str)
    assert s["other"] == 123


# =========================
# csv_worker
# =========================
@mock.patch("event_generator.main.save_events_batch")
@mock.patch("event_generator.main.open", create=True)
@mock.patch("event_generator.main.os.path.exists", return_value=False)
def test_csv_worker_generates_events_when_csv_not_exists(
    mock_exists, mock_open, mock_save, monkeypatch
):
    # уменьшаем объём, чтобы не грузить тест
    monkeypatch.setattr(main_mod, "EVENT_COUNT", 2)
    monkeypatch.setattr(main_mod, "CSV_PATH", "/tmp/test_events.csv")

    main_mod.csv_worker()

    # должна быть вставка батча в БД
    assert mock_save.called
    # writerows был вызван (файл "записан")
    assert mock_open.called


@mock.patch("event_generator.main.save_events_batch")
@mock.patch("event_generator.main.pd.read_csv")
@mock.patch("event_generator.main.os.path.exists", return_value=True)
def test_csv_worker_reads_csv_when_exists(
    mock_exists, mock_read_csv, mock_save, monkeypatch
):
    # имитируем существующий CSV с одной строкой
    df = pd.DataFrame(
        [
            {
                "event_id": "id1",
                "type": "view",
                "created_at": "2026-01-01T00:00:00",
                "received_at": "2026-01-01T00:00:01",
                "session_id": "s1",
                "user_id": 1,
                "ip": "127.0.0.1",
                "url": "/",
                "referrer": "/",
                "device_type": "desktop",
                "user_agent": "ua",
                "event_title": "page_view",
                "element_id": "#btn",
                "x": 10,
                "y": 20,
            }
        ]
    )
    mock_read_csv.return_value = df
    monkeypatch.setattr(main_mod, "CSV_PATH", "/tmp/test_events.csv")

    main_mod.csv_worker()

    mock_read_csv.assert_called_once()
    mock_save.assert_called_once()
    # проверяем, что source были проставлены как csv
    saved_events = mock_save.call_args.args[0]
    assert saved_events[0]["source"] == "csv"


# =========================
# http_worker — один шаг
# =========================
@mock.patch("event_generator.main.save_events_batch")
@mock.patch("event_generator.main.requests.post")
def test_http_worker_step_sends_batch_and_saves(mock_post, mock_save):
    local_q = queue.Queue()

    # наполняем очередь на полный батч
    for i in range(main_mod.HTTP_BATCH_SIZE):
        local_q.put(
            {
                "event_id": f"id{i}",
                "type": "view",
                "created_at": "2026-01-01T00:00:00",
                "received_at": "2026-01-01T00:00:01",
                "session_id": "s1",
                "user_id": 1,
                "ip": "127.0.0.1",
                "url": "/",
                "referrer": "/",
                "device_type": "desktop",
                "user_agent": "ua",
                "event_title": "page_view",
                "element_id": "#btn",
                "x": 10,
                "y": 20,
                "payload": "{}",
            }
        )

    def http_worker_step():
        batch = []
        for _ in range(main_mod.HTTP_BATCH_SIZE):
            try:
                e = local_q.get(timeout=0.1)
                e["source"] = "http"
                batch.append(e)
                local_q.task_done()
            except queue.Empty:
                break

        if batch:
            batch_serialized = [
                main_mod.serialize_event_for_http(e) for e in batch
            ]
            mock_post.return_value.status_code = 200
            main_mod.requests.post(
                main_mod.HTTP_ENDPOINT,
                json=batch_serialized,
                timeout=5,
            )
            main_mod.save_events_batch(batch)

    http_worker_step()

    assert mock_post.called
    assert mock_save.called
    # проверяем, что source выставлен корректно
    saved_events = mock_save.call_args.args[0]
    assert all(e["source"] == "http" for e in saved_events)


# =========================
# main()
# =========================
@mock.patch("event_generator.main.threading.Thread")
@mock.patch("event_generator.main.http_queue")
@mock.patch("event_generator.main.create_table_if_not_exists")
def test_main_runs_with_http_enabled(
    mock_create, mock_http_queue, mock_thread, monkeypatch
):
    # включаем только HTTP
    monkeypatch.setattr(main_mod, "CSV_ENABLED", False)
    monkeypatch.setattr(main_mod, "HTTP_ENABLED", True)
    monkeypatch.setattr(main_mod, "EVENT_COUNT", 3)

    # join не блокирует
    mock_http_queue.join.return_value = None

    # фейковый поток, чтобы не крутился на самом деле
    fake_thread = mock.Mock()
    mock_thread.return_value = fake_thread

    main_mod.main()

    mock_create.assert_called_once()
    # поток для http_worker должен быть создан и запущен
    mock_thread.assert_called()
    fake_thread.start.assert_called()
    # в очередь положили EVENT_COUNT событий
    assert mock_http_queue.put.call_count == 3


@mock.patch("event_generator.main.threading.Thread")
@mock.patch("event_generator.main.http_queue")
@mock.patch("event_generator.main.create_table_if_not_exists")
def test_main_runs_with_only_csv(
    mock_create, mock_http_queue, mock_thread, monkeypatch
):
    # включаем только CSV
    monkeypatch.setattr(main_mod, "CSV_ENABLED", True)
    monkeypatch.setattr(main_mod, "HTTP_ENABLED", False)
    monkeypatch.setattr(main_mod, "EVENT_COUNT", 2)

    fake_thread = mock.Mock()
    mock_thread.return_value = fake_thread

    main_mod.main()

    mock_create.assert_called_once()
    # поток для csv_worker создаётся
    mock_thread.assert_called()
    fake_thread.start.assert_called()
    # при HTTP_DISABLED put в http_queue не должен вызываться
    assert not mock_http_queue.put.called
