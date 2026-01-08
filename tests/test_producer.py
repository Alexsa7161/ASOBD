import json
from unittest.mock import patch, MagicMock
import producer.rabbit_producer as rp


def test_generate_event():
    event = rp.generate_event()

    assert isinstance(event, dict)
    assert "event_id" in event
    assert "type" in event
    assert "payload" in event
    assert event["source"] == "rabbitmq"

    payload = event["payload"]
    assert "event_title" in payload
    assert "x" in payload
    assert "y" in payload


@patch("producer.rabbit_producer.time.sleep", return_value=None)
@patch("producer.rabbit_producer.pika.BlockingConnection")
def test_main_sends_events(mock_connection, _):
    mock_channel = MagicMock()
    mock_connection.return_value.channel.return_value = mock_channel

    rp.EVENT_COUNT = 2
    rp.RATE = 1

    rp.main()

    mock_channel.queue_declare.assert_called_once_with(
        queue=rp.RABBITMQ_QUEUE,
        durable=True
    )

    assert mock_channel.basic_publish.called

    body = mock_channel.basic_publish.call_args.kwargs["body"]
    event = json.loads(body)

    assert "event_id" in event
    assert event["source"] == "rabbitmq"
