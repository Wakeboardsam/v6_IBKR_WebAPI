import re

def main():
    with open("v6_IBKR_WebAPI/engine/engine.py", "r") as f:
        text = f.read()

    new_code = """        if stale_cancelled:
            logger.info("Stale session orders canceled. Skipping Bridge Anchor and normal grid evaluations for this tick to let state settle.")
            return

        # Bridge Anchor safety invariant:
        # Bridge Anchor must never remain live and hidden when the protective row 7 SELL is gone.
        if self.order_manager.has_open_action(7, 'BRIDGE_BUY'):
            row_7_sell_active = False
            for o in open_orders:
                oid = str(o['order_id'])
                if oid in broker_order_ids and self.order_manager.is_tracked(oid):
                    row, action = self.order_manager.get_row_and_action(oid)
                    if row == 7 and action == 'SELL':
                        row_7_sell_active = True
                        break

            if not row_7_sell_active:
                logger.error("Bridge Anchor safety violation: Active BRIDGE_BUY found for row 7, but no actual row 7 SELL order exists at broker. Canceling BRIDGE_BUY.")
                # Cancel the bridge buy orders
                bridge_oids = self.order_manager.get_order_ids_for_action(7, 'BRIDGE_BUY')
                for oid in bridge_oids:
                    await self.broker.cancel_order(oid)
                self.order_manager.clear_action_for_row(7, 'BRIDGE_BUY')
                if self.grid_state and 7 in self.grid_state.rows:
                    current_status = self.grid_state.rows[7].status
                    new_status = _remove_status_part(current_status, 'BRIDGE_BUY:')
                    self._update_row_status_in_memory(7, new_status)
                    import asyncio
                    asyncio.create_task(self._sync_to_sheet())
                return
"""
    text = text.replace(
        "        if stale_cancelled:\n            logger.info(\"Stale session orders canceled. Skipping Bridge Anchor and normal grid evaluations for this tick to let state settle.\")\n            return\n",
        new_code
    )
    with open("v6_IBKR_WebAPI/engine/engine.py", "w") as f:
        f.write(text)

if __name__ == "__main__":
    main()
