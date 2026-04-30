import logging
from typing import Dict, Set, Tuple, List, Optional, Any, Callable
from datetime import datetime, timedelta
from brokers.base import OrderResult

logger = logging.getLogger(__name__)

class OrderManager:
    def __init__(self):
        # Mapping of row_index to set of active order_ids (parent and child)
        self._row_to_orders: Dict[Any, Set[str]] = {}
        # Mapping of order_id to (row_index, action)
        self._order_map: Dict[str, Tuple[Any, str]] = {}
        # Mapping of row_index to action ('BUY' or 'SELL')
        self._row_actions: Dict[Any, str] = {}
        # Tombstone history for cancelled/errored orders: order_id -> (row_index, action, reason, timestamp)
        self._tombstones: Dict[str, Tuple[Any, str, str, datetime]] = {}
        # TTL for tombstones (24 hours)
        self.tombstone_ttl = timedelta(hours=24)

    def track(self, row_index: Any, order_result: OrderResult, action: str = None,
              broker: Optional[Any] = None, on_update: Optional[Callable] = None):
        """
        Track one or more orders for a grid row.
        order_result.order_id can be a single ID or multiple IDs separated by '|'.
        If broker and on_update are provided, subscribes to updates for each order.
        """
        if action:
            self._row_actions[row_index] = action.upper()

        order_ids = order_result.order_id.split('|')

        if row_index not in self._row_to_orders:
            self._row_to_orders[row_index] = set()

        for oid in order_ids:
            self._row_to_orders[row_index].add(oid)
            self._order_map[oid] = (row_index, self._row_actions[row_index])
            if broker and on_update:
                broker.subscribe_to_updates(oid, on_update)

        logger.info(f"Tracking {self._row_actions[row_index]} row {row_index} with order(s): {order_ids}")

    def has_open_buy(self, row_index: Any) -> bool:
        return row_index in self._row_to_orders and self._row_actions.get(row_index) == "BUY"

    def has_open_sell(self, row_index: Any) -> bool:
        return row_index in self._row_to_orders and self._row_actions.get(row_index) == "SELL"

    def _prune_tombstones(self):
        now = datetime.now()
        expired = [oid for oid, data in self._tombstones.items() if now - data[3] > self.tombstone_ttl]
        for oid in expired:
            del self._tombstones[oid]

    def mark_filled(self, order_id: str) -> Tuple[Optional[Any], Optional[str]]:
        return self._remove_order(order_id, "filled")

    def mark_cancelled(self, order_id: str) -> Tuple[Optional[Any], Optional[str]]:
        return self._remove_order(order_id, "cancelled")

    def _remove_order(self, order_id: str, reason: str) -> Tuple[Optional[Any], Optional[str]]:
        if order_id in self._order_map:
            row_index, action = self._order_map.pop(order_id)

            # Save to tombstones to preserve historical mapping
            if reason in ("cancelled", "error"):
                self._tombstones[order_id] = (row_index, action, reason, datetime.now())
                self._prune_tombstones()

            if row_index in self._row_to_orders:
                self._row_to_orders[row_index].discard(order_id)
                if not self._row_to_orders[row_index]:
                    # All orders for this row are gone (either filled or cancelled)
                    del self._row_to_orders[row_index]
                    logger.info(f"Row {row_index} is now clear (last order {order_id} was {reason})")
                else:
                    logger.info(f"Order {order_id} for row {row_index} was {reason}. Remaining orders for row: {self._row_to_orders[row_index]}")
            return row_index, action
        return None, None

    def get_tracked_order_ids(self) -> List[str]:
        return list(self._order_map.keys())

    def is_tracked(self, order_id: str) -> bool:
        return order_id in self._order_map

    def is_tombstoned(self, order_id: str) -> bool:
        return order_id in self._tombstones

    def get_row_and_action(self, order_id: str) -> Tuple[Optional[Any], Optional[str]]:
        if order_id in self._order_map:
            return self._order_map[order_id]
        elif order_id in self._tombstones:
            tombstone = self._tombstones[order_id]
            logger.info(f"Recovered mapping for order {order_id} from tombstones (row {tombstone[0]}, {tombstone[1]})")
            return tombstone[0], tombstone[1]
        return None, None
