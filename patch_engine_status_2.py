import re

def main():
    with open("v6_IBKR_WebAPI/engine/engine.py", "r") as f:
        text = f.read()

    text = text.replace(
"""                    elif action == 'BRIDGE_BUY':
                        logger.error(f"BRIDGE_BUY order {order_id} errored. Returning row 7 to WORKING_SELL and reverting bridge state.")
                        self._bridge_state = 'IDLE'
                        if self.grid_state and row_index in self.grid_state.rows:
                            status = self.grid_state.rows[row_index].status
                            parts = status.split('|')
                            new_status = '|'.join([p for p in parts if not p.startswith('BRIDGE_BUY:')])
                        else:
                            if self.grid_state and row_index in self.grid_state.rows:
                                current_status = self.grid_state.rows[row_index].status
                                new_status = _remove_status_part(current_status, "WORKING_SELL:")
                            else:
                                new_status = "IDLE\"""",
"""                    elif action == 'BRIDGE_BUY':
                        logger.error(f"BRIDGE_BUY order {order_id} errored. Returning row 7 to WORKING_SELL and reverting bridge state.")
                        self._bridge_state = 'IDLE'
                        if self.grid_state and row_index in self.grid_state.rows:
                            current_status = self.grid_state.rows[row_index].status
                            new_status = _remove_status_part(current_status, 'BRIDGE_BUY:')
                        else:
                            new_status = "IDLE\""""
    )

    text = text.replace(
"""                    elif action == 'BRIDGE_BUY':
                        logger.info(f"BRIDGE_BUY order {order_id} cancelled explicitly. Reverting row 7 status.")
                        self._bridge_state = 'IDLE'
                        if self.grid_state and row_index in self.grid_state.rows:
                            status = self.grid_state.rows[row_index].status
                            parts = status.split('|')
                            new_status = '|'.join([p for p in parts if not p.startswith('BRIDGE_BUY:')])
                        else:
                            if self.grid_state and row_index in self.grid_state.rows:
                                current_status = self.grid_state.rows[row_index].status
                                new_status = _remove_status_part(current_status, "WORKING_SELL:")
                            else:
                                new_status = "IDLE\"""",
"""                    elif action == 'BRIDGE_BUY':
                        logger.info(f"BRIDGE_BUY order {order_id} cancelled explicitly. Reverting row 7 status.")
                        self._bridge_state = 'IDLE'
                        if self.grid_state and row_index in self.grid_state.rows:
                            current_status = self.grid_state.rows[row_index].status
                            new_status = _remove_status_part(current_status, 'BRIDGE_BUY:')
                        else:
                            new_status = "IDLE\""""
    )

    with open("v6_IBKR_WebAPI/engine/engine.py", "w") as f:
        f.write(text)

if __name__ == "__main__":
    main()
