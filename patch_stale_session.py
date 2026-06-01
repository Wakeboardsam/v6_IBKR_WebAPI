import re

def main():
    with open("v6_IBKR_WebAPI/engine/engine.py", "r") as f:
        text = f.read()

    new_code = """        # Quick scan for active TRIM_SELL to re-establish bridge state before circuit breaker
        if self._bridge_state != 'TRIM_PENDING':
            for row in self.grid_state.rows.values():
                status_parts = row.status.split('|')
                for part in status_parts:
                    if part.startswith("TRIM_SELL:"):
                        trim_order_id = part.split(":")[1]
                        if trim_order_id in broker_order_ids and not self.order_manager.is_tracked(trim_order_id):
                            logger.info(f"Re-tracking TRIM_SELL order {trim_order_id} from sheet status for row {row.row_index}")
                            self.order_manager.track(row.row_index, OrderResult(order_id=trim_order_id, status='submitted'), 'TRIM_SELL',
                                                broker=self.broker, on_update=self._handle_order_update)

                            self._bridge_state = 'TRIM_PENDING'
                            for open_o in open_orders:
                                if str(open_o['order_id']) == trim_order_id:
                                    self._pending_trim_qty = open_o.get('qty', 0)
                                    break

                            if not self._pending_trim_qty:
                                sheet_shares = sum(r.shares for r in self.grid_state.rows.values() if r.has_y)
                                if broker_shares > sheet_shares:
                                    self._pending_trim_qty = broker_shares - sheet_shares
                                else:
                                    logger.error("Re-tracked TRIM_SELL but no excess shares exist. Halting bridge flow.")
                                    self._bridge_state = 'BRIDGE_HALTED'
                                    return

                            logger.info(f"Restored TRIM_PENDING state with pending trim quantity: {self._pending_trim_qty}")
                            break

        # Explicit stale-session cleanup
        from brokers.ibkr.order_builder import get_dynamic_exchange, get_dynamic_tif
        current_desired_exchange = get_dynamic_exchange()
        current_desired_tif = get_dynamic_tif(current_desired_exchange)

        stale_cancelled = False
        for o in open_orders:
            oid = str(o['order_id'])
            if self.order_manager.is_tracked(oid):
                o_exchange = o.get('exchange')
                o_tif = o.get('tif')

                # Check if it's an outdated session order
                if current_desired_exchange == 'SMART' and (o_exchange == 'OVERNIGHT' or o_tif == 'DAY'):
                    logger.info(f"Canceling stale session tracked order {oid} ({o_exchange}/{o_tif} -> {current_desired_exchange}/{current_desired_tif})")
                    await self.broker.cancel_order(oid)
                    stale_cancelled = True
                # If we ever transition the other way, we'd also clean it up:
                elif current_desired_exchange == 'OVERNIGHT' and (o_exchange == 'SMART' or o_tif == 'GTC'):
                    logger.info(f"Canceling stale session tracked order {oid} ({o_exchange}/{o_tif} -> {current_desired_exchange}/{current_desired_tif})")
                    await self.broker.cancel_order(oid)
                    stale_cancelled = True

        if stale_cancelled:
            logger.info("Stale session orders canceled. Skipping Bridge Anchor and normal grid evaluations for this tick to let state settle.")
            return

"""
    text = text.replace(
        "        # Quick scan for active TRIM_SELL to re-establish bridge state before circuit breaker\n        if self._bridge_state != 'TRIM_PENDING':\n            for row in self.grid_state.rows.values():\n                status_parts = row.status.split('|')\n                for part in status_parts:\n                    if part.startswith(\"TRIM_SELL:\"):\n                        trim_order_id = part.split(\":\")[1]\n                        if trim_order_id in broker_order_ids and not self.order_manager.is_tracked(trim_order_id):\n                            logger.info(f\"Re-tracking TRIM_SELL order {trim_order_id} from sheet status for row {row.row_index}\")\n                            self.order_manager.track(row.row_index, OrderResult(order_id=trim_order_id, status='submitted'), 'TRIM_SELL',\n                                                broker=self.broker, on_update=self._handle_order_update)\n\n                            self._bridge_state = 'TRIM_PENDING'\n                            for open_o in open_orders:\n                                if str(open_o['order_id']) == trim_order_id:\n                                    self._pending_trim_qty = open_o.get('qty', 0)\n                                    break\n\n                            if not self._pending_trim_qty:\n                                sheet_shares = sum(r.shares for r in self.grid_state.rows.values() if r.has_y)\n                                if broker_shares > sheet_shares:\n                                    self._pending_trim_qty = broker_shares - sheet_shares\n                                else:\n                                    logger.error(\"Re-tracked TRIM_SELL but no excess shares exist. Halting bridge flow.\")\n                                    self._bridge_state = 'BRIDGE_HALTED'\n                                    return\n\n                            logger.info(f\"Restored TRIM_PENDING state with pending trim quantity: {self._pending_trim_qty}\")\n                            break\n",
        new_code
    )
    with open("v6_IBKR_WebAPI/engine/engine.py", "w") as f:
        f.write(text)

if __name__ == "__main__":
    main()
