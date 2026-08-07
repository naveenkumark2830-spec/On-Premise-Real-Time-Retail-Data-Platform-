from pathlib import Path
import pandas as pd


class DataLoader:

    def __init__(self):

        self.project_root = Path(__file__).resolve().parent.parent
        self.raw_data_dir = self.project_root / "data" / "OLIST_Raw"

        self.customers = None
        self.products = None
        self.payments = None
        self.orders = None
        self.order_items = None

        # Lookup dictionaries
        self.customer_lookup = {}
        self.payment_lookup = {}
        self.order_items_lookup = {}

    def load_csv(self, filename):

        file_path = self.raw_data_dir / filename
        return pd.read_csv(file_path)

    def build_indexes(self):

        print("\nBuilding Lookup Indexes...\n")

        self.customer_lookup = (
            self.customers
            .set_index("customer_id")
            .to_dict("index")
        )

        self.payment_lookup = (
            self.payments
            .groupby("order_id")
            .first()
            .to_dict("index")
        )

        self.order_items_lookup = (
            self.order_items
            .groupby("order_id")
            .first()
            .to_dict("index")
        )

        print("Indexes Created Successfully.")

    def load_all(self):

        print("Loading datasets...\n")

        self.customers = self.load_csv("olist_customers_dataset.csv")
        self.products = self.load_csv("olist_products_dataset.csv")
        self.payments = self.load_csv("olist_order_payments_dataset.csv")
        self.orders = self.load_csv("olist_orders_dataset.csv")
        self.order_items = self.load_csv("olist_order_items_dataset.csv")

        self.build_indexes()

        print("\nDatasets Loaded Successfully.")