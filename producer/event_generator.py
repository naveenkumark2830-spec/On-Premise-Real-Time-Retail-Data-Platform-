from datetime import datetime


class EventGenerator:

    def __init__(self, loader):
        self.loader = loader

    def generate_event(self):

        while True:

            # Pick a random order
            order = self.loader.orders.sample(1).iloc[0]

            # Safely fetch related records
            customer = self.loader.customer_lookup.get(order["customer_id"])
            payment = self.loader.payment_lookup.get(order["order_id"])
            item = self.loader.order_items_lookup.get(order["order_id"])

            # If any related record is missing, try another order
            if customer is None or payment is None or item is None:
                continue

            event = {
                "order_id": order["order_id"],
                "customer_id": order["customer_id"],
                "customer_city": customer["customer_city"],
                "customer_state": customer["customer_state"],
                "product_id": item["product_id"],
                "seller_id": item["seller_id"],
                "price": float(item["price"]),
                "freight_value": float(item["freight_value"]),
                "payment_type": payment["payment_type"],
                "payment_value": float(payment["payment_value"]),
                "order_status": order["order_status"],
                "event_timestamp": datetime.now().isoformat()
            }

            return event