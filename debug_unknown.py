import asyncio
from unittest.mock import AsyncMock, patch
from v6_IBKR_WebAPI.engine.engine import GridEngine
from v6_IBKR_WebAPI.config.schema import AppConfig
from v6_IBKR_WebAPI.engine.grid_state import GridState, GridRow
import logging

def setup_grid_state(rows_data):
    rows = {}
    for r in rows_data:
        r_idx = r['row_index']
        rows[r_idx] = GridRow(
            row_index=r_idx,
            shares=r.get('shares', 50),
            buy_price=r.get('buy_price', 100.0),
            sell_price=r.get('sell_price', 105.0),
            status=r.get('status', 'IDLE'),
            has_y=r.get('status', 'IDLE').startswith("OWNED:") or r.get('status', 'IDLE').startswith("WORKING_SELL:")
        )
    return GridState(rows=rows)

config = AppConfig(
    enable_bridge_anchor=True,
    bridge_max_auto_trim_shares=5,
    anchor_buy_offset=1.5,
    google_sheet_id="test",
    google_credentials_json="{}"
)

mock_broker = AsyncMock()
mock_sheet = AsyncMock()

# Make is_exec_id_seen synchronous return value or correctly awaited
mock_sheet.is_exec_id_seen = lambda x: False

engine = GridEngine(mock_broker, mock_sheet, config)
engine.grid_state = setup_grid_state([
    {'row_index': 7, 'status': 'WORKING_SELL:7', 'shares': 50, 'sell_price': 105.0}
])

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

logging.basicConfig(level=logging.DEBUG)
engine._handle_execution(exec_data)
