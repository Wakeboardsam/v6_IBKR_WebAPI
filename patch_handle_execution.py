import re

def main():
    with open("v6_IBKR_WebAPI/brokers/ibkr/adapter.py", "r") as f:
        text = f.read()

    new_exec_details = """            exec_data = {
                "exec_id": execution.execId,
                "order_id": str(execution.orderId),
                "perm_id": str(execution.permId),
                "symbol": trade.contract.symbol,
                "type": action,
                "filled_qty": int(execution.shares),
                "filled_price": float(execution.price),
                "order_type": trade.order.orderType,
                "tif": trade.order.tif,
                "aux_price": trade.order.auxPrice,
                "limit_price": trade.order.lmtPrice,
                "exchange": trade.contract.exchange,
                "action": trade.order.action
            }"""

    old_exec_details = """            exec_data = {
                "exec_id": execution.execId,
                "order_id": str(execution.orderId),
                "perm_id": str(execution.permId),
                "symbol": trade.contract.symbol,
                "type": action,
                "filled_qty": int(execution.shares),
                "filled_price": float(execution.price)
            }"""

    text = text.replace(old_exec_details, new_exec_details)
    with open("v6_IBKR_WebAPI/brokers/ibkr/adapter.py", "w") as f:
        f.write(text)

    with open("v6_IBKR_WebAPI/engine/engine.py", "r") as f:
        text = f.read()

    new_handle_exec = """        order_id = exec_data.get("order_id", "")
        row_index, action = self.order_manager.get_row_and_action(order_id)

        # If the order manager knows the action, use it, otherwise use the side from the execution event
        final_action = action if action else exec_data.get("type", "UNKNOWN")

        exec_data["row_id"] = str(row_index) if row_index is not None else "UNKNOWN"
        exec_data["type"] = final_action

        if exec_data["row_id"] == "UNKNOWN":
            # Detect suspected untracked Bridge Anchor fills
            order_type = str(exec_data.get("order_type", "")).upper()
            tif = exec_data.get("tif", "")
            filled_qty = exec_data.get("filled_qty", 0)
            aux_price = exec_data.get("aux_price")
            side = exec_data.get("type", "")

            is_bridge_suspect = False
            if side == 'BUY' and ('STP' in order_type or 'STOP' in order_type) and tif == 'GTC':
                if self.grid_state and 7 in self.grid_state.rows:
                    row7_shares = self.grid_state.rows[7].shares
                    row7_sell_target = self.grid_state.rows[7].sell_price
                    if abs(filled_qty - row7_shares) < 0.01:
                        if aux_price is not None and abs(aux_price - row7_sell_target) < 0.02:
                            is_bridge_suspect = True

            if is_bridge_suspect:
                logger.critical(f"CRITICAL: Untracked Bridge Anchor order filled! OrderID: {order_id}, ExecID: {exec_id}, Side: {side}, Shares: {filled_qty}, Price: {exec_data.get('filled_price')}. This will cause a permanent share mismatch until manually resolved.")
            else:
                logger.warning(f"Queueing execution {exec_id} for untracked order {order_id} (row UNKNOWN).")
        else:
            logger.info(f"Queueing execution {exec_id} for order {order_id} (row {exec_data['row_id']})")
"""
    old_handle_exec = """        order_id = exec_data.get("order_id", "")
        row_index, action = self.order_manager.get_row_and_action(order_id)

        # If the order manager knows the action, use it, otherwise use the side from the execution event
        final_action = action if action else exec_data.get("type", "UNKNOWN")

        exec_data["row_id"] = str(row_index) if row_index is not None else "UNKNOWN"
        exec_data["type"] = final_action

        logger.info(f"Queueing execution {exec_id} for order {order_id} (row {exec_data['row_id']})")"""

    text = text.replace(old_handle_exec, new_handle_exec)
    with open("v6_IBKR_WebAPI/engine/engine.py", "w") as f:
        f.write(text)

if __name__ == "__main__":
    main()
