import re

def main():
    with open("v6_IBKR_WebAPI/engine/engine.py", "r") as f:
        text = f.read()

    # Add helper function at the top of engine.py
    helper = """
def _remove_status_part(status: str, prefix: str) -> str:
    parts = status.split('|')
    kept = [p for p in parts if not p.startswith(prefix)]
    if not any(p.startswith('OWNED:') for p in kept):
        kept.insert(0, "OWNED:0")
    return '|'.join(kept)

class GridEngine:
"""
    text = text.replace("class GridEngine:", helper)

    # 1. Action == SELL filled
    # Replace new_status = "IDLE" with status preservation logic
    # Find:
    #                     else:
    #                         new_status = "IDLE"
    text = text.replace(
"""                        else:
                            new_status = "IDLE\"""",
"""                        else:
                            if self.grid_state and row_index in self.grid_state.rows:
                                current_status = self.grid_state.rows[row_index].status
                                new_status = _remove_status_part(current_status, "WORKING_SELL:")
                            else:
                                new_status = "IDLE\""""
    )

    # 2. Action == SELL cancelled / error
    # We want to replace new_status = f"OWNED:{owned_id}" with _remove_status_part
    old_sell_err_canc = """                    if action == 'SELL':
                        # Try to find existing ID or use 0
                        owned_id = "0"
                        if self.grid_state and row_index in self.grid_state.rows:
                            status = self.grid_state.rows[row_index].status
                            if "OWNED:" in status:
                                owned_id = status.split("OWNED:")[1].split("|")[0]
                        new_status = f"OWNED:{owned_id}\""""

    new_sell_err_canc = """                    if action == 'SELL':
                        if self.grid_state and row_index in self.grid_state.rows:
                            current_status = self.grid_state.rows[row_index].status
                            new_status = _remove_status_part(current_status, 'WORKING_SELL:')
                        else:
                            new_status = "OWNED:0\""""
    text = text.replace(old_sell_err_canc, new_sell_err_canc)

    old_sell_canc_explicit = """                    if action == 'SELL':
                        owned_id = "0"
                        if self.grid_state and row_index in self.grid_state.rows:
                            status = self.grid_state.rows[row_index].status
                            if "OWNED:" in status:
                                owned_id = status.split("OWNED:")[1].split("|")[0]
                        new_status = f"OWNED:{owned_id}\""""
    text = text.replace(old_sell_canc_explicit, new_sell_err_canc)

    # 3. Action == BRIDGE_BUY error
    old_bridge_err = """                    elif action == 'BRIDGE_BUY':
                        logger.error(f"BRIDGE_BUY order {order_id} errored. Returning row 7 to WORKING_SELL and reverting bridge state.")
                        self._bridge_state = 'IDLE'
                        if self.grid_state and row_index in self.grid_state.rows:
                            status = self.grid_state.rows[row_index].status
                            parts = status.split('|')
                            new_status = '|'.join([p for p in parts if not p.startswith('BRIDGE_BUY:')])
                        else:
                            new_status = "IDLE\""""
    new_bridge_err = """                    elif action == 'BRIDGE_BUY':
                        logger.error(f"BRIDGE_BUY order {order_id} errored. Returning row 7 to WORKING_SELL and reverting bridge state.")
                        self._bridge_state = 'IDLE'
                        if self.grid_state and row_index in self.grid_state.rows:
                            current_status = self.grid_state.rows[row_index].status
                            new_status = _remove_status_part(current_status, 'BRIDGE_BUY:')
                        else:
                            new_status = "IDLE\""""
    text = text.replace(old_bridge_err, new_bridge_err)

    # 4. Action == BRIDGE_BUY cancelled
    old_bridge_canc = """                    elif action == 'BRIDGE_BUY':
                        logger.info(f"BRIDGE_BUY order {order_id} cancelled explicitly. Reverting row 7 status.")
                        self._bridge_state = 'IDLE'
                        if self.grid_state and row_index in self.grid_state.rows:
                            status = self.grid_state.rows[row_index].status
                            parts = status.split('|')
                            new_status = '|'.join([p for p in parts if not p.startswith('BRIDGE_BUY:')])
                        else:
                            new_status = "IDLE\""""
    new_bridge_canc = """                    elif action == 'BRIDGE_BUY':
                        logger.info(f"BRIDGE_BUY order {order_id} cancelled explicitly. Reverting row 7 status.")
                        self._bridge_state = 'IDLE'
                        if self.grid_state and row_index in self.grid_state.rows:
                            current_status = self.grid_state.rows[row_index].status
                            new_status = _remove_status_part(current_status, 'BRIDGE_BUY:')
                        else:
                            new_status = "IDLE\""""
    text = text.replace(old_bridge_canc, new_bridge_canc)

    with open("v6_IBKR_WebAPI/engine/engine.py", "w") as f:
        f.write(text)

if __name__ == "__main__":
    main()
