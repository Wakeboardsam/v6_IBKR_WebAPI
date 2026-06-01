import re

def main():
    with open("v6_IBKR_WebAPI/tests/test_engine.py", "r") as f:
        text = f.read()

    text = text.replace("    sell_calls = [c for c in mock_broker.place_limit_order.mock_calls if c.kwargs.get('action') == 'SELL']\n        assert len(sell_calls) == 1",
                        "    sell_calls = [c for c in mock_broker.place_limit_order.mock_calls if c.kwargs.get('action') == 'SELL']\n    assert len(sell_calls) == 1")
    text = text.replace("    buy_calls = [c for c in mock_broker.place_limit_order.mock_calls if c.kwargs.get('action') == 'BUY']\n        assert len(buy_calls) == 1",
                        "    buy_calls = [c for c in mock_broker.place_limit_order.mock_calls if c.kwargs.get('action') == 'BUY']\n    assert len(buy_calls) == 1")

    with open("v6_IBKR_WebAPI/tests/test_engine.py", "w") as f:
        f.write(text)

if __name__ == "__main__":
    main()
