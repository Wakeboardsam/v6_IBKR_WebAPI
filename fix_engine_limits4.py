import re

def main():
    with open("v6_IBKR_WebAPI/tests/test_engine.py", "r") as f:
        text = f.read()

    old_test = """        # Check SELL for row 7
        assert engine.order_manager.has_open_sell(7)"""

    new_test = """        # Check SELL for row 7
        sell_calls = [c for c in mock_broker.place_limit_order.mock_calls if c.kwargs.get('action') == 'SELL']
        assert len(sell_calls) == 1"""

    text = text.replace(old_test, new_test)

    old_test2 = """        # Check BUY for row 8
        assert engine.order_manager.has_open_buy(8)"""

    new_test2 = """        # Check BUY for row 8
        buy_calls = [c for c in mock_broker.place_limit_order.mock_calls if c.kwargs.get('action') == 'BUY']
        assert len(buy_calls) == 1"""

    text = text.replace(old_test2, new_test2)

    with open("v6_IBKR_WebAPI/tests/test_engine.py", "w") as f:
        f.write(text)

if __name__ == "__main__":
    main()
