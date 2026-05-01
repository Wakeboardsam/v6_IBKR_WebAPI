import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime

from engine.engine import GridEngine
from engine.grid_state import GridState, GridRow
from config.schema import AppConfig
from brokers.base import OrderResult

@pytest.fixture
def mock_config():
    config = MagicMock(spec=AppConfig)
    config.max_spread_pct = 0.5
    config.anchor_buy_offset = 0.10
    config.share_mismatch_mode = "warn"
    return config

@pytest.fixture
def mock_broker():
    broker = AsyncMock()
    broker.get_wallet_balance.return_value = 10000.0
    broker.get_price.return_value = 50.0
    # snapshot that says flat
    snapshot = MagicMock()
    snapshot.is_ready = True
    snapshot.positions = {"TQQQ": 0}
    broker.get_position_snapshot.return_value = snapshot
    broker.get_open_orders.return_value = []

    broker.get_next_order_id.return_value = "100"
    broker.place_limit_order.return_value = OrderResult(order_id="100", status="submitted")
    return broker

@pytest.fixture
def mock_sheet():
    sheet = AsyncMock()

    # GridState setup
    state = GridState(rows={})
    # Row 7 is IDLE
    state.rows[7] = GridRow(row_index=7, shares=100, buy_price=49.90, sell_price=50.10, status="IDLE", has_y=False)
    sheet.fetch_grid.return_value = state

    return sheet

@pytest.mark.asyncio
async def test_startup_flat_refresh(mock_broker, mock_sheet, mock_config):
    # Test A: startup/manual flat refresh
    mock_broker.get_bid_ask.return_value = (49.95, 50.05)

    engine = GridEngine(broker=mock_broker, sheet=mock_sheet, config=mock_config)
    engine.last_broker_shares = 0 # emulate already flat
    engine._is_weekend_gap = False
    mock_broker.get_bid_ask.return_value = (49.95, 50.05)

    await engine._tick()

    # Assert G7 was written with ask 50.05
    mock_sheet.write_anchor_ask.assert_called_once_with(50.05)
    assert engine.anchor_refresh_pending is True
    # Assert no order was placed
    mock_broker.place_limit_order.assert_not_called()

@pytest.mark.asyncio
async def test_next_tick_after_refresh(mock_broker, mock_sheet, mock_config):
    # Test B: next tick after refresh
    engine = GridEngine(broker=mock_broker, sheet=mock_sheet, config=mock_config)
    engine.last_broker_shares = 0
    engine.anchor_refresh_pending = True
    engine._is_weekend_gap = False
    mock_broker.get_bid_ask.return_value = (49.95, 50.05)

    await engine._tick()

    # Assert G7 was NOT written again
    mock_sheet.write_anchor_ask.assert_not_called()
    # Assert order WAS placed with recalculated sheet values (buy_price + offset)
    # buy_price in mock is 49.90, offset is 0.10 => 50.00
    mock_broker.place_limit_order.assert_called_once()
    args, kwargs = mock_broker.place_limit_order.call_args
    assert kwargs['action'] == 'BUY'
    assert kwargs['qty'] == 100
    assert kwargs['limit_price'] == 50.00
    # Flag should be cleared
    assert engine.anchor_refresh_pending is False

@pytest.mark.asyncio
async def test_active_anchor_exists(mock_broker, mock_sheet, mock_config):
    # Test C: Active anchor exists
    engine = GridEngine(broker=mock_broker, sheet=mock_sheet, config=mock_config)
    engine.last_broker_shares = 0
    engine.anchor_refresh_pending = False
    engine._is_weekend_gap = False

    # Mock that row 7 has an active open buy
    engine.order_manager.track(7, OrderResult(order_id="99", status="submitted"), "BUY", broker=mock_broker, on_update=engine._handle_order_update)

    await engine._tick()

    # Assert G7 was not written
    mock_sheet.write_anchor_ask.assert_not_called()
    # Assert place order not called (duplicate)
    mock_broker.place_limit_order.assert_not_called()
    # Flag should remain False
    assert engine.anchor_refresh_pending is False

@pytest.mark.asyncio
async def test_bad_quote_spread(mock_broker, mock_sheet, mock_config):
    # Test D: delayed/bad quote
    mock_broker.get_bid_ask.return_value = (0, 0) # Bad quote

    engine = GridEngine(broker=mock_broker, sheet=mock_sheet, config=mock_config)
    engine.last_broker_shares = 0
    engine.anchor_refresh_pending = False
    engine._is_weekend_gap = False

    await engine._tick()

    # Assert G7 was not written
    mock_sheet.write_anchor_ask.assert_not_called()
    # Assert no order was placed
    mock_broker.place_limit_order.assert_not_called()
    # Flag should remain False since we skipped
    assert engine.anchor_refresh_pending is False

    # Test wide spread
    mock_broker.get_bid_ask.return_value = (40.0, 50.0) # very wide spread
    await engine._tick()
    mock_sheet.write_anchor_ask.assert_not_called()
    mock_broker.place_limit_order.assert_not_called()
    assert engine.anchor_refresh_pending is False

@pytest.mark.asyncio
async def test_pending_true_but_bad_quote(mock_broker, mock_sheet, mock_config):
    # Test: anchor_refresh_pending=True + bad/wide spread => no BUY, pending remains True
    engine = GridEngine(broker=mock_broker, sheet=mock_sheet, config=mock_config)
    engine.last_broker_shares = 0
    engine.anchor_refresh_pending = True
    engine._is_weekend_gap = False

    mock_broker.get_bid_ask.return_value = (40.0, 50.0) # wide spread

    await engine._tick()

    # Should not write G7 again
    mock_sheet.write_anchor_ask.assert_not_called()
    # Should not place BUY
    mock_broker.place_limit_order.assert_not_called()
    # Pending should remain True
    assert engine.anchor_refresh_pending is True

@pytest.mark.asyncio
async def test_g7_write_failure(mock_broker, mock_sheet, mock_config):
    # Test: G7 write failure => pending does not become True, no stale anchor BUY is placed
    engine = GridEngine(broker=mock_broker, sheet=mock_sheet, config=mock_config)
    engine.last_broker_shares = 0
    engine.anchor_refresh_pending = False
    engine._is_weekend_gap = False

    mock_broker.get_bid_ask.return_value = (49.95, 50.05)
    mock_sheet.write_anchor_ask.side_effect = Exception("Google Sheets API error")

    await engine._tick()

    # Should attempt to write G7
    mock_sheet.write_anchor_ask.assert_called_once_with(50.05)
    # But because it failed, pending should remain False
    assert engine.anchor_refresh_pending is False
    # And we shouldn't place a BUY
    mock_broker.place_limit_order.assert_not_called()
