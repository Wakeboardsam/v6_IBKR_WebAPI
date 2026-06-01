import re

def main():
    with open("v6_IBKR_WebAPI/tests/test_bridge_anchor_bugfixes.py", "r") as f:
        text = f.read()

    new_test = """@pytest.mark.asyncio
async def test_unknown_bridge_anchor_execution_alert(mock_broker, mock_sheet, config):
    engine = GridEngine(mock_broker, mock_sheet, config)

    engine.grid_state = setup_grid_state([
        {'row_index': 7, 'status': 'WORKING_SELL:7', 'shares': 50, 'sell_price': 105.0}
    ])
    mock_sheet.is_exec_id_seen = lambda x: False

    # We need to test the execution handler
    exec_data = {
        "exec_id": "exec_221",
        "order_id": "221",
        "perm_id": "perm_221",
        "symbol": "TQQQ",
        "type": "BUY",
        "filled_qty": 50,
        "filled_price": 105.01,
        "order_type": "STP LMT",
        "tif": "GTC",
        "aux_price": 105.0,
        "limit_price": 106.0,
        "exchange": "SMART",
        "action": "BUY"
    }

    with patch('engine.engine.logger.critical') as mock_critical:
        with patch('asyncio.create_task') as mock_create_task:
            engine._handle_execution(exec_data)

            # Verify critical log was called containing 'Untracked Bridge Anchor order filled'
            mock_critical.assert_called_once()
            assert "Untracked Bridge Anchor order filled" in mock_critical.call_args[0][0]"""

    old_test = """@pytest.mark.asyncio
async def test_unknown_bridge_anchor_execution_alert(mock_broker, mock_sheet, config):
    engine = GridEngine(mock_broker, mock_sheet, config)

    engine.grid_state = setup_grid_state([
        {'row_index': 7, 'status': 'WORKING_SELL:7', 'shares': 50, 'sell_price': 105.0}
    ])
    mock_sheet.is_exec_id_seen.return_value = False

    # We need to test the execution handler
    exec_data = {
        "exec_id": "exec_221",
        "order_id": "221",
        "perm_id": "perm_221",
        "symbol": "TQQQ",
        "type": "BUY",
        "filled_qty": 50,
        "filled_price": 105.01,
        "order_type": "STP LMT",
        "tif": "GTC",
        "aux_price": 105.0,
        "limit_price": 106.0,
        "exchange": "SMART",
        "action": "BUY"
    }

    with patch('engine.engine.logger.critical') as mock_critical:
        engine._handle_execution(exec_data)

        # Verify critical log was called containing 'Untracked Bridge Anchor order filled'
        mock_critical.assert_called_once()
        assert "Untracked Bridge Anchor order filled" in mock_critical.call_args[0][0]"""

    text = text.replace(old_test, new_test)

    with open("v6_IBKR_WebAPI/tests/test_bridge_anchor_bugfixes.py", "w") as f:
        f.write(text)

if __name__ == "__main__":
    main()
