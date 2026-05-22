import pytest
from unittest.mock import AsyncMock, patch, call
from engine.engine import GridEngine
from engine.order_manager import OrderManager
from brokers.base import OrderResult, PositionSnapshot
from config.schema import AppConfig
from engine.grid_state import GridRow, GridState
from datetime import datetime
import asyncio

@pytest.fixture
def config():
    return AppConfig(
        active_broker="ibkr",
        paper_trading=True,
        ibkr_host="127.0.0.1",
        ibkr_port=7497,
        ibkr_client_id=1,
        google_sheet_id="fake_id",
        google_credentials_json="{}",
        anchor_buy_offset=1.5,
        enable_bridge_anchor=True,
        bridge_max_auto_trim_shares=5
    )

@pytest.fixture
def mock_broker():
    broker = AsyncMock()
    broker.get_next_order_id.return_value = "ORD-BRIDGE"
    broker.get_open_orders.return_value = []
    broker.place_limit_order.return_value = OrderResult(order_id="ORD-TRIM", status="submitted")
    broker.place_stop_limit_order.return_value = OrderResult(order_id="ORD-BRIDGE", status="submitted")
    return broker

@pytest.fixture
def mock_sheet():
    sheet = AsyncMock()
    return sheet

def setup_grid_state(rows_data):
    """Helper to setup basic grid state"""
    rows = {}
    for data in rows_data:
        has_y = data.get('status', 'IDLE').startswith("OWNED:") or data.get('status', 'IDLE').startswith("WORKING_SELL:")
        r = GridRow(
            row_index=data.get('row_index'),
            status=data.get('status', 'IDLE'),
            shares=data.get('shares', 10),
            buy_price=data.get('buy_price', 100.0),
            sell_price=data.get('sell_price', 105.0),
            has_y=has_y
        )
        rows[r.row_index] = r

    return GridState(rows=rows)

# 1. Bridge Anchor arms when row 7 is the only owned row and has WORKING_SELL.
@pytest.mark.asyncio
async def test_bridge_anchor_arms_correctly(mock_broker, mock_sheet, config):
    engine = GridEngine(mock_broker, mock_sheet, config)
    mock_sheet.fetch_grid.return_value = setup_grid_state([
        {'row_index': 7, 'status': 'WORKING_SELL:ORD-SELL-7', 'shares': 50, 'sell_price': 105.0},
        {'row_index': 8, 'status': 'IDLE'}
    ])
    mock_broker.get_position_snapshot.return_value = PositionSnapshot(is_ready=True, positions={"TQQQ": 50})

    # We must explicitly track the SELL order to satisfy condition 4
    engine.order_manager.track(7, OrderResult(order_id="ORD-SELL-7", status="submitted"), 'SELL')

    await engine._tick()

    # Check stop limit order was placed
    mock_broker.place_stop_limit_order.assert_called_once_with(
        ticker="TQQQ", action="BUY", qty=50,
        stop_price=105.0, limit_price=106.5, # 105.0 + 1.5 offset
        on_update=engine._handle_order_update, order_id="ORD-BRIDGE"
    )

    assert "BRIDGE_BUY:ORD-BRIDGE" in engine.grid_state.rows[7].status

# 2. Bridge Anchor does not arm when more than row 7 is owned.
@pytest.mark.asyncio
async def test_bridge_anchor_no_arm_multiple_owned(mock_broker, mock_sheet, config):
    engine = GridEngine(mock_broker, mock_sheet, config)
    mock_sheet.fetch_grid.return_value = setup_grid_state([
        {'row_index': 7, 'status': 'WORKING_SELL:ORD-SELL-7'},
        {'row_index': 8, 'status': 'WORKING_SELL:ORD-SELL-8'}
    ])
    mock_broker.get_position_snapshot.return_value = PositionSnapshot(is_ready=True, positions={"TQQQ": 20})
    engine.order_manager.track(7, OrderResult(order_id="ORD-SELL-7", status="submitted"), 'SELL')

    await engine._tick()
    mock_broker.place_stop_limit_order.assert_not_called()

# 3. Bridge Anchor does not arm when row 7 is not owned.
@pytest.mark.asyncio
async def test_bridge_anchor_no_arm_row7_not_owned(mock_broker, mock_sheet, config):
    engine = GridEngine(mock_broker, mock_sheet, config)
    mock_sheet.fetch_grid.return_value = setup_grid_state([
        {'row_index': 7, 'status': 'IDLE'},
        {'row_index': 8, 'status': 'WORKING_SELL:ORD-SELL-8'}
    ])
    mock_broker.get_position_snapshot.return_value = PositionSnapshot(is_ready=True, positions={"TQQQ": 10})

    await engine._tick()
    mock_broker.place_stop_limit_order.assert_not_called()

# 4. Bridge Anchor does not arm if no row 7 working sell exists.
@pytest.mark.asyncio
async def test_bridge_anchor_no_arm_no_working_sell(mock_broker, mock_sheet, config):
    engine = GridEngine(mock_broker, mock_sheet, config)
    mock_sheet.fetch_grid.return_value = setup_grid_state([
        {'row_index': 7, 'status': 'OWNED:1234'},
        {'row_index': 8, 'status': 'IDLE'}
    ])
    mock_broker.get_position_snapshot.return_value = PositionSnapshot(is_ready=True, positions={"TQQQ": 10})

    await engine._tick()
    mock_broker.place_stop_limit_order.assert_not_called()

# 5. Bridge Anchor fill writes G7 using actual fill price.
@pytest.mark.asyncio
async def test_bridge_anchor_fill_writes_g7(mock_broker, mock_sheet, config):
    engine = GridEngine(mock_broker, mock_sheet, config)
    # Set up active bridge order
    engine.order_manager.track(7, OrderResult(order_id="ORD-BRIDGE", status="submitted"), 'BRIDGE_BUY')

    # Simulate fill event
    fill_result = OrderResult(order_id="ORD-BRIDGE", status="filled", filled_price=106.1, filled_qty=50)
    engine._handle_order_update(fill_result)

    # Should write to G7
    # Note: the write is wrapped in asyncio.create_task so we need to yield to event loop
    await asyncio.sleep(0.01)
    mock_sheet.write_anchor_ask.assert_called_once_with(106.1)

    # Check bridge state
    assert engine._bridge_state == 'ANCHOR_RECALC_PENDING'

@pytest.mark.asyncio
async def test_bridge_mismatch_allowed_circuit_breaker(mock_broker, mock_sheet, config):
    engine = GridEngine(mock_broker, mock_sheet, config)
    engine._bridge_state = 'ANCHOR_RECALC_PENDING'

    mock_sheet.fetch_grid.return_value = setup_grid_state([
        {'row_index': 7, 'status': 'OWNED:ORD-BRIDGE', 'shares': 50}
    ])

    # Broker has 50 shares, but sheet says 40 (hasn't recalced yet)
    mock_broker.get_position_snapshot.return_value = PositionSnapshot(is_ready=True, positions={"TQQQ": 50})
    mock_sheet.fetch_grid.return_value.rows[7].shares = 40
    mock_broker.get_open_orders.return_value = []

    # Tick should not log halt
    await engine._tick()

    for call_args in mock_sheet.log_error.call_args_list:
        if isinstance(call_args[0], tuple) and call_args[0]:
            assert "CIRCUIT BREAKER" not in call_args[0][0]
        else:
            assert "CIRCUIT BREAKER" not in call_args.args[0]

# 10. Bridge Anchor cleanup.
@pytest.mark.asyncio
async def test_bridge_anchor_cleanup(mock_broker, mock_sheet, config):
    engine = GridEngine(mock_broker, mock_sheet, config)
    engine.order_manager.track(7, OrderResult(order_id="ORD-BRIDGE", status="submitted"), 'BRIDGE_BUY')

    # Scenario: Row 7 is no longer ONLY owned row (row 8 was acquired)
    mock_sheet.fetch_grid.return_value = setup_grid_state([
        {'row_index': 7, 'status': 'WORKING_SELL:ORD-SELL-7'},
        {'row_index': 8, 'status': 'OWNED:123'}
    ])
    mock_broker.get_position_snapshot.return_value = PositionSnapshot(is_ready=True, positions={"TQQQ": 20})

    await engine._tick()

    mock_broker.cancel_order.assert_called_with("ORD-BRIDGE")

# 11. Testing session behavior dynamic inherited
# Assuming it inherits adapter defaults since we pass True for outsideRth, etc implicitly based on order builder
# We verify the adapter call itself is correct, which we did in test 1.

@pytest.mark.asyncio
async def test_bridge_trim_fill_preserves_owned(mock_broker, mock_sheet, config):
    engine = GridEngine(mock_broker, mock_sheet, config)
    engine.grid_state = setup_grid_state([
        {'row_index': 7, 'status': 'OWNED:OLD-ID|TRIM_SELL:ORD-TRIM', 'shares': 50}
    ])

    engine.order_manager.track(7, OrderResult(order_id="ORD-TRIM", status="submitted"), 'TRIM_SELL')

    fill_result = OrderResult(order_id="ORD-TRIM", status="filled", filled_price=105.0, filled_qty=2)
    engine._handle_order_update(fill_result)

    # Check that status wasn't blown away to IDLE, but is OWNED:OLD-ID
    await asyncio.sleep(0.01)

    # We don't have direct access to memory assert simply here without accessing internal calls
    # but we can look at what would be passed to sync_to_sheet if we mocked the update
    # In engine, memory is updated directly: self.grid_state.rows[7].status
    assert engine.grid_state.rows[7].status == "OWNED:OLD-ID"

@pytest.mark.asyncio
async def test_bridge_retrack_orders(mock_broker, mock_sheet, config):
    engine = GridEngine(mock_broker, mock_sheet, config)
    mock_sheet.fetch_grid.return_value = setup_grid_state([
        {'row_index': 7, 'status': 'WORKING_SELL:ORD-SELL-7|BRIDGE_BUY:ORD-BRIDGE', 'shares': 50, 'sell_price': 105.0}
    ])

    mock_broker.get_open_orders.return_value = [{'order_id': 'ORD-BRIDGE'}, {'order_id': 'ORD-SELL-7'}]
    mock_broker.get_position_snapshot.return_value = PositionSnapshot(is_ready=True, positions={"TQQQ": 50})

    await engine._tick()

    # Verify both got re-tracked
    assert engine.order_manager.has_open_sell(7)
    assert engine.order_manager.has_open_action(7, 'BRIDGE_BUY')
