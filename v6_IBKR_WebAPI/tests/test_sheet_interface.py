import unittest
from unittest.mock import MagicMock, patch
import json
import asyncio
from config.schema import AppConfig
from sheets.interface import SheetInterface
from sheets.schema import (
    COL_STATUS, COL_STRATEGY, COL_SELL_PRICE, COL_BUY_PRICE, COL_SHARES,
    ROW_HEARTBEAT, COL_HEARTBEAT, ROW_CASH, COL_CASH, ROW_ANCHOR_ASK, COL_ANCHOR_ASK,
    GRID_TAB_NAME, FILLS_TAB_NAME, HEALTH_TAB_NAME, ERRORS_TAB_NAME
)
import gspread

class TestSheetInterface(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = AppConfig(
            google_sheet_id="fake_id",
            google_credentials_json=json.dumps({"project_id": "test", "private_key": "fake", "client_email": "test@example.com"})
        )
        # Mocking gspread.authorize and google.oauth2.service_account.Credentials.from_service_account_info
        self.patcher_creds = patch('google.oauth2.service_account.Credentials.from_service_account_info')
        self.patcher_gspread = patch('gspread.authorize')

        self.mock_creds = self.patcher_creds.start()
        self.mock_gspread = self.patcher_gspread.start()

        self.mock_client = MagicMock()
        self.mock_gspread.return_value = self.mock_client
        self.mock_sheet = MagicMock()
        self.mock_client.open_by_key.return_value = self.mock_sheet

        self.interface = SheetInterface(self.config)

    def tearDown(self):
        self.patcher_creds.stop()
        self.patcher_gspread.stop()

    async def test_fetch_grid_success(self):
        mock_worksheet = MagicMock()
        self.mock_sheet.worksheet.return_value = mock_worksheet

        # data for range C7:H100
        # Columns: C(Status), D(Strategy), E(Empty), F(Sell), G(Buy), H(Shares)
        mock_worksheet.get_values.return_value = [
            ["Ready", "Y", "", "105.0", "100.0", "10"],
            ["Filled", "N", "", "115.0", "110.0", "15"],
            ["Ready", "Y", "", "125.0", "120.0", "20"],
        ]

        grid_state = await self.interface.fetch_grid()

        self.assertEqual(len(grid_state.rows), 3)
        self.assertEqual(grid_state.rows[7].status, "Ready")
        self.assertTrue(grid_state.rows[7].has_y)
        self.assertEqual(grid_state.rows[7].sell_price, 105.0)
        self.assertEqual(grid_state.rows[7].buy_price, 100.0)
        self.assertEqual(grid_state.rows[7].shares, 10)

        self.assertFalse(grid_state.rows[8].has_y)
        self.assertEqual(grid_state.distal_y_row, 9)

    async def test_fetch_grid_has_y_parsing(self):
        mock_worksheet = MagicMock()
        self.mock_sheet.worksheet.return_value = mock_worksheet

        # Test cases specifically for has_y logic
        # Columns: C(Status), D(Strategy/Live), E(Empty), F(Sell), G(Buy), H(Shares)
        mock_worksheet.get_values.return_value = [
            # 7: OWNED:13 + blank D -> True
            ["OWNED:13", "", "", "105.0", "100.0", "65"],
            # 8: WORKING_SELL:13 + blank D -> True
            ["WORKING_SELL:13", "", "", "115.0", "110.0", "15"],
            # 9: WORKING_BUY:13 + blank D -> False
            ["WORKING_BUY:13", "", "", "125.0", "120.0", "20"],
            # 10: IDLE + blank D -> False
            ["IDLE", "", "", "135.0", "130.0", "10"],
            # 11: IDLE + Y -> True
            ["IDLE", "Y", "", "145.0", "140.0", "5"],
        ]

        grid_state = await self.interface.fetch_grid()

        self.assertEqual(len(grid_state.rows), 5)

        # 7: OWNED:13 + blank D -> True
        row_7 = grid_state.rows[7]
        self.assertEqual(row_7.status, "OWNED:13")
        self.assertTrue(row_7.has_y, "OWNED should imply has_y is True")
        self.assertEqual(row_7.shares, 65)

        # 8: WORKING_SELL:13 + blank D -> True
        row_8 = grid_state.rows[8]
        self.assertEqual(row_8.status, "WORKING_SELL:13")
        self.assertTrue(row_8.has_y, "WORKING_SELL should imply has_y is True")

        # 9: WORKING_BUY:13 + blank D -> False
        row_9 = grid_state.rows[9]
        self.assertEqual(row_9.status, "WORKING_BUY:13")
        self.assertFalse(row_9.has_y, "WORKING_BUY should NOT imply has_y is True")

        # 10: IDLE + blank D -> False
        row_10 = grid_state.rows[10]
        self.assertEqual(row_10.status, "IDLE")
        self.assertFalse(row_10.has_y)

        # 11: IDLE + Y -> True
        row_11 = grid_state.rows[11]
        self.assertEqual(row_11.status, "IDLE")
        self.assertTrue(row_11.has_y, "Explicit 'Y' in column D should mean has_y is True")

    async def test_update_row_status(self):
        mock_worksheet = MagicMock()
        self.mock_sheet.worksheet.return_value = mock_worksheet

        await self.interface.update_row_status(10, "Working")

        mock_worksheet.update_cell.assert_called_once_with(10, COL_STATUS, "Working")

    async def test_write_heartbeat(self):
        mock_worksheet = MagicMock()
        self.mock_sheet.worksheet.return_value = mock_worksheet

        await self.interface.write_heartbeat("OK")

        mock_worksheet.update_cell.assert_called_once_with(ROW_HEARTBEAT, COL_HEARTBEAT, "OK")

    async def test_write_cash_value(self):
        mock_worksheet = MagicMock()
        self.mock_sheet.worksheet.return_value = mock_worksheet

        await self.interface.write_cash_value(1234.56)

        mock_worksheet.update_cell.assert_called_once_with(ROW_CASH, COL_CASH, 1234.56)

    async def test_write_anchor_ask(self):
        mock_worksheet = MagicMock()
        self.mock_sheet.worksheet.return_value = mock_worksheet

        await self.interface.write_anchor_ask(55.5)

        mock_worksheet.update_cell.assert_called_once_with(ROW_ANCHOR_ASK, COL_ANCHOR_ASK, 55.5)

    async def test_unauthorized_write_grid(self):
        mock_worksheet = MagicMock()
        self.mock_sheet.worksheet.return_value = mock_worksheet

        # Column D (COL_STRATEGY) is not authorized for write
        with self.assertRaises(ValueError) as cm:
            self.interface._update_cell_with_guard(GRID_TAB_NAME, 7, COL_STRATEGY, "Y")

        self.assertIn("Unauthorized write attempt", str(cm.exception))

    async def test_unauthorized_worksheet(self):
        with self.assertRaises(ValueError) as cm:
            self.interface._update_cell_with_guard("UnknownTab", 1, 1, "data")

        self.assertIn("Unauthorized worksheet", str(cm.exception))

    async def test_append_only_guard(self):
        with self.assertRaises(ValueError) as cm:
            self.interface._update_cell_with_guard(FILLS_TAB_NAME, 1, 1, "data")

        self.assertIn("Use append_row", str(cm.exception))

    async def test_log_fill_success(self):
        # Now log_fill just puts it in the queue
        fill_data = {
            "exec_id": "EXEC1",
            "row_id": "7",
            "type": "BUY",
            "filled_price": 99.5,
            "filled_qty": 10,
            "order_id": "ORDER-123",
            "perm_id": "PERM1",
            "symbol": "TQQQ"
        }

        result = await self.interface.log_fill(fill_data)
        self.assertTrue(result)
        self.assertFalse(self.interface._fill_queue.empty())

        # Verify queue contents
        item = await self.interface._fill_queue.get()
        self.assertEqual(item["row_id"], "7")
        self.assertEqual(item["type"], "BUY")

    async def test_log_error_success(self):
        mock_worksheet = MagicMock()
        mock_worksheet.get_values.return_value = [["Header"]] # Not empty
        self.mock_sheet.worksheet.return_value = mock_worksheet

        result = await self.interface.log_error("Test error")

        self.assertTrue(result)
        mock_worksheet.append_row.assert_called_once()
        args = mock_worksheet.append_row.call_args[0][0]
        self.assertEqual(args[1], "Test error")

    async def test_log_headers_when_empty(self):
        mock_worksheet = MagicMock()
        mock_worksheet.get_values.return_value = [] # Empty
        self.mock_sheet.worksheet.return_value = mock_worksheet

        # Should trigger header append
        result = await self.interface.log_error("Test error")

        self.assertTrue(result)
        # First call should be append_row with headers
        # Second call should be append_row with data
        self.assertEqual(mock_worksheet.append_row.call_count, 2)
        header_args = mock_worksheet.append_row.call_args_list[0][0][0]
        self.assertEqual(header_args, ["TIMESTAMP", "ERROR_MSG"])

    async def test_log_error_missing_worksheet(self):
        self.mock_sheet.worksheet.side_effect = gspread.exceptions.WorksheetNotFound

        with patch('sheets.interface.logger') as mock_logger:
            result = await self.interface.log_error("Test error")

            self.assertFalse(result)
            mock_logger.error.assert_any_call("Worksheet 'Errors' not found in the spreadsheet.")

    async def test_log_health_appends_net_liquidation_value(self):
        mock_worksheet = MagicMock()
        # Non-empty sheet so headers aren't appended here
        mock_worksheet.get_values.return_value = [["Header"]]
        # Optional: present headers so any header checks don't try to expand
        mock_worksheet.row_values.return_value = [
            "TIMESTAMP", "LAST_PRICE", "OPEN_ORDERS_COUNT", "LAST_FILL_TIME", "STATUS",
            "POSITION", "MARKET_PRICE", "MARKET_VALUE", "AVG_COST", "NET_LIQUIDATION_VALUE"
        ]
        self.mock_sheet.worksheet.return_value = mock_worksheet

        health_data = {
            "last_price": 100.0,
            "open_orders_count": 3,
            "last_fill_time": "Never",
            "status": "Running",
            "position": 10,
            "market_price": 101.0,
            "market_value": 1010.0,
            "avg_cost": 99.0,
            "net_liquidation_value": 12345.67,
        }

        result = await self.interface.log_health(health_data)
        self.assertTrue(result)

        args = mock_worksheet.append_row.call_args[0][0]
        self.assertEqual(len(args), 10)
        self.assertEqual(args[-1], 12345.67)
