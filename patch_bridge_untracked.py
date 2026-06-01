import re

def main():
    with open("v6_IBKR_WebAPI/engine/engine.py", "r") as f:
        text = f.read()

    new_code = """                    self._update_row_status_in_memory(7, new_status)
                    import asyncio
                    asyncio.create_task(self._sync_to_sheet())
                return

        # Detect and cancel untracked or duplicate Bridge Anchor orders at the broker
        bridge_like_orders = []
        if self.grid_state and 7 in self.grid_state.rows:
            row7_shares = self.grid_state.rows[7].shares
            row7_sell_target = self.grid_state.rows[7].sell_price

            for o in open_orders:
                # A Bridge-Anchor-like order is:
                # - ticker == TQQQ
                # - action == BUY
                # - order_type is STP LMT / StopLimitOrder / etc
                # - tif == GTC
                # - qty approximately equals row 7 shares
                # - aux_price / stop price approximately equals row 7 sell target

                ticker = o.get('ticker', '')
                action = o.get('action', '')
                order_type = str(o.get('order_type', '')).upper()
                tif = o.get('tif', '')
                qty = o.get('qty', 0)
                aux_price = o.get('aux_price')

                if ticker == 'TQQQ' and action == 'BUY' and ('STP' in order_type or 'STOP' in order_type) and tif == 'GTC':
                    # Check qty and price tolerances
                    if abs(qty - row7_shares) < 0.01:
                        if aux_price is not None and abs(aux_price - row7_sell_target) < 0.02:
                            bridge_like_orders.append(o)

        untracked_or_duplicate_cancelled = False
        valid_tracked_bridge_id = None
        if self.grid_state and 7 in self.grid_state.rows:
            status = self.grid_state.rows[7].status
            valid_id = _extract_order_id_from_status(status, "BRIDGE_BUY:")
            if valid_id and self.order_manager.is_tracked(valid_id):
                valid_tracked_bridge_id = valid_id

        for o in bridge_like_orders:
            oid = str(o['order_id'])
            if oid != valid_tracked_bridge_id:
                logger.warning(f"Cancelling untracked/stale Bridge Anchor order {oid}")
                await self.broker.cancel_order(oid)
                untracked_or_duplicate_cancelled = True

        if untracked_or_duplicate_cancelled:
            logger.info("Untracked/duplicate Bridge Anchors canceled. Skipping evaluations for this tick.")
            return

        if broker_shares != sheet_shares:
"""
    text = text.replace(
        "                    self._update_row_status_in_memory(7, new_status)\n                    import asyncio\n                    asyncio.create_task(self._sync_to_sheet())\n                return\n\n        if broker_shares != sheet_shares:\n",
        new_code
    )
    with open("v6_IBKR_WebAPI/engine/engine.py", "w") as f:
        f.write(text)

if __name__ == "__main__":
    main()
