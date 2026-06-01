import re

def main():
    with open("v6_IBKR_WebAPI/tests/test_engine.py", "r") as f:
        text = f.read()

    # Need to check `mock_broker.place_limit_order.call_args_list` because `has_open_sell` fails.
    # We replaced it previously but apparently the script didn't apply properly if multiple tests matched.

    # Wait, the failure says `assert engine.order_manager.has_open_sell(7)`
    # Which means my replacement did NOT apply to test_engine_places_sell_and_buy_limits!

    text = text.replace("assert engine.order_manager.has_open_sell(7)", "sell_calls = [c for c in mock_broker.place_limit_order.mock_calls if c.kwargs.get('action') == 'SELL']\n        assert len(sell_calls) == 1")
    text = text.replace("assert engine.order_manager.has_open_buy(8)", "buy_calls = [c for c in mock_broker.place_limit_order.mock_calls if c.kwargs.get('action') == 'BUY']\n        assert len(buy_calls) == 1")

    with open("v6_IBKR_WebAPI/tests/test_engine.py", "w") as f:
        f.write(text)

if __name__ == "__main__":
    main()
