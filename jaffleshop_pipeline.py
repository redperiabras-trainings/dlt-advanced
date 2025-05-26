import dlt
import os

from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

os.environ["EXTRACT__WORKERS"] = "3"
os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "5000"

base_url = "https://jaffle-shop.scalevector.ai/api/v1"
page_size = 1_000


@dlt.source(name="jaffle_shop")
def jaffle_shop_source():

    client = RESTClient(
        base_url=base_url,
        paginator=PageNumberPaginator(page_param="page", base_page=1, total_path=None),
    )

    @dlt.resource(name="customers", primary_key="id", parallelized=True)
    def get_customers():
        for page in client.paginate("customers"):
            yield page

    @dlt.resource(name="products", primary_key="sku", parallelized=True)
    def get_products():
        for page in client.paginate("products"):
            yield page

    @dlt.resource(name="orders", primary_key="id", parallelized=True)
    def get_orders():
        for page in client.paginate("orders", params={"page_size": page_size}):
            yield page

    return [get_customers, get_orders, get_products]


def main() -> None:
    pipeline = dlt.pipeline(
        destination="duckdb",
        dataset_name="optimized-jaffle_shop",
        full_refresh=True,
        progress="log",
    )

    pipeline.extract(jaffle_shop_source())
    pipeline.normalize()
    load_info = pipeline.load()
    
    print(load_info)

if __name__ == "__main__":
    main()
