import re

def main():
    with open("v6_IBKR_WebAPI/tests/test_engine.py", "r") as f:
        text = f.read()

    # Fix test_anchor_acquisition and test_full_sell_cycle_same_shares
    # These probably fail because we added `get_dynamic_exchange()` to the stale session logic
    # but didn't mock `get_dynamic_exchange` in these tests.

    # Or they fail because `place_limit_order` was called with unexpected arguments?
    # Actually, `test_anchor_acquisition` has: `Expected: place_limit_order(..., order_id='ORD-123')`
    # But it got `order_id='100'` from our `mock_broker.get_next_order_id.side_effect = ['100', '101']` addition!

    # Wait, earlier I did:
    # mock_broker.get_wallet_balance.return_value = 50000.0
    # mock_broker.get_next_order_id.side_effect = ['100', '101']
    # And I replaced all instances in the file! That affected ALL tests that set get_wallet_balance!

    # Let's revert that global change and only fix `test_engine_places_sell_and_buy_limits`

    text = text.replace(
        "mock_broker.get_wallet_balance.return_value = 50000.0\n    mock_broker.get_next_order_id.side_effect = ['100', '101']",
        "mock_broker.get_wallet_balance.return_value = 50000.0"
    )

    # Only in test_engine_places_sell_and_buy_limits
    old_test = """    @pytest.mark.asyncio
    async def test_engine_places_sell_and_buy_limits(mock_broker, mock_sheet, config):
        engine = GridEngine(mock_broker, mock_sheet, config)
        # distal_y will be 7. Window [7, 10].
        # Row 7 is has_y -> should place SELL.
        # Row 8 is NOT has_y and 8 > 7 -> should place BUY.

        mock_broker.get_position_snapshot.return_value = PositionSnapshot(is_ready=True, positions={"TQQQ": 10}) # Matches Row 7 shares
        mock_broker.get_wallet_balance.return_value = 50000.0"""

    new_test = """    @pytest.mark.asyncio
    async def test_engine_places_sell_and_buy_limits(mock_broker, mock_sheet, config):
        engine = GridEngine(mock_broker, mock_sheet, config)
        # distal_y will be 7. Window [7, 10].
        # Row 7 is has_y -> should place SELL.
        # Row 8 is NOT has_y and 8 > 7 -> should place BUY.

        mock_broker.get_position_snapshot.return_value = PositionSnapshot(is_ready=True, positions={"TQQQ": 10}) # Matches Row 7 shares
        mock_broker.get_wallet_balance.return_value = 50000.0
        mock_broker.get_next_order_id.side_effect = ['100', '101']"""

    text = text.replace(old_test, new_test)

    with open("v6_IBKR_WebAPI/tests/test_engine.py", "w") as f:
        f.write(text)

if __name__ == "__main__":
    main()
