with open("v6_IBKR_WebAPI/engine/order_manager.py", "r") as f:
    text = f.read()

text = text.replace(
    "    def has_open_buy(self, row_index: Any) -> bool:\n        return self.has_open_action(row_index, 'BUY') or (row_index in self._row_to_orders and self._row_actions.get(row_index) == \"BUY\")",
    "    def has_open_buy(self, row_index: Any) -> bool:\n        return self.has_open_action(row_index, 'BUY')"
)

text = text.replace(
    "    def has_open_sell(self, row_index: Any) -> bool:\n        return self.has_open_action(row_index, 'SELL') or (row_index in self._row_to_orders and self._row_actions.get(row_index) == \"SELL\")",
    "    def has_open_sell(self, row_index: Any) -> bool:\n        return self.has_open_action(row_index, 'SELL')"
)

with open("v6_IBKR_WebAPI/engine/order_manager.py", "w") as f:
    f.write(text)
