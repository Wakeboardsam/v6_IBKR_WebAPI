import re

def main():
    with open("v6_IBKR_WebAPI/tests/test_bridge_anchor_bugfixes.py", "r") as f:
        text = f.read()

    new_imports = """import pytest
from unittest.mock import AsyncMock, MagicMock, patch"""

    old_imports = """import pytest
from unittest.mock import AsyncMock, MagicMock"""

    text = text.replace(old_imports, new_imports)

    # ensure mock_sheet.fetch_grid doesn't return mock object for _handle_execution
    # which is not async
    test_unknown = """@pytest.mark.asyncio
async def test_unknown_bridge_anchor_execution_alert(mock_broker, mock_sheet, config):
    engine = GridEngine(mock_broker, mock_sheet, config)

    engine.grid_state = setup_grid_state([
        {'row_index': 7, 'status': 'WORKING_SELL:7', 'shares': 50, 'sell_price': 105.0}
    ])

    # We need to test the execution handler
    exec_data = {"""

    old_unknown = """@pytest.mark.asyncio
async def test_unknown_bridge_anchor_execution_alert(mock_broker, mock_sheet, config):
    engine = GridEngine(mock_broker, mock_sheet, config)

    mock_sheet.fetch_grid.return_value = setup_grid_state([
        {'row_index': 7, 'status': 'WORKING_SELL:7', 'shares': 50, 'sell_price': 105.0}
    ])

    # We need to test the execution handler
    exec_data = {"""

    text = text.replace(old_unknown, test_unknown)

    with open("v6_IBKR_WebAPI/tests/test_bridge_anchor_bugfixes.py", "w") as f:
        f.write(text)

if __name__ == "__main__":
    main()
