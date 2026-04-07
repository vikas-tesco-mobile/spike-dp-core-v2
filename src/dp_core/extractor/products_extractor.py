import argparse
import json
from typing import Dict, Any, List

from dp_core.extractor.api_caller import APICaller
from dp_core.utils.api_auth import BearerAuth
from dp_core.utils.config_loader import load_configs
from dp_core.utils.databricks_utils import write_json
from dp_core.utils.logger import get_logger
from dp_core.utils.time_utils import build_dates_for_path

logger = get_logger(__name__)


class ProductsExtractor:
    def __init__(self, configs: dict, auth: BearerAuth):
        self.configs = configs
        self.auth = auth
        self.product_list_config = self.configs.get("product_list")
        self.products_by_id_config = self.configs.get("products_by_id")
        self.target_path = self.configs.get("target_path")
        self.batch_size = self.configs.get("batch_size")
        self.default_headers = {
            "User-Agent": "dp-finance-products-extractor/1.0",
            "Accept": "application/json",
        }

    @staticmethod
    def _group_keys_in_batches(result_list: List[str], batch_sie: int):
        return [
            result_list[i : i + batch_sie]
            for i in range(0, len(result_list), batch_sie)
        ]

    def _get_product_list(self) -> List[str]:
        product_list_url = self.product_list_config.get("url")  # type: ignore[union-attr]
        request_params = self.product_list_config.get("request_params") or {}  # type: ignore[union-attr]
        auth_header = self.auth.get_auth_header()
        headers = {**self.default_headers, **auth_header}

        product_unique_keys: set[str] = set()
        page_counter = 0

        section_codes = request_params.get("sectionCode")
        if isinstance(section_codes, str):
            section_codes = [section_codes]
        elif section_codes is None:
            raise ValueError("Section codes are not provided")

        for code in section_codes:
            this_params = dict(request_params)
            this_params["sectionCode"] = code
            logger.info(f"➡ Starting fetch for sectionCode: {code}")

            api_caller = APICaller(product_list_url, headers)
            section_count_before = len(product_unique_keys)

            for page in api_caller.paginate_by_next_page(this_params):
                page_counter += 1
                products = page.get("productResults", [])
                for product in products:
                    unique_key = product.get("uniqueKey")
                    if unique_key:
                        product_unique_keys.add(unique_key)

            section_count_after = len(product_unique_keys) - section_count_before
            logger.info(
                f"Finished fetch for sectionCode: {code}, "
                f"retrieved {section_count_after} unique keys"
            )

        logger.info(
            f"Finished listing products. Total unique product keys retrieved: {len(product_unique_keys)} "
            f"across {len(section_codes)} sectionCode values"
        )
        return list(product_unique_keys)

    def _get_products_by_id(self, product_unique_keys: List[str]) -> Dict[str, Any]:
        products_by_id_url = self.products_by_id_config.get("url")  # type: ignore[union-attr]
        auth_header = self.auth.get_auth_header()
        headers = {**self.default_headers, **auth_header}
        api_caller = APICaller(products_by_id_url, headers)
        request_params = {"uniqueKey": product_unique_keys}
        response = api_caller.get(params=request_params)
        return response

    def _save_response_json(self, resp_json: dict, batch_num: int):
        ingestion_date, ingestion_hour, time_str = build_dates_for_path()
        file_path = f"{self.target_path}/{ingestion_date}/{ingestion_hour}/products_{time_str}_{batch_num}.json"
        json_text = json.dumps(resp_json)
        write_json(file_path, json_text)
        logger.info(f"✔ Saved raw JSON response to {file_path}")

    def run(self):
        product_list = self._get_product_list()
        product_batches = self._group_keys_in_batches(product_list, self.batch_size)
        logger.info("Getting product details")
        for product_batch in product_batches:
            batch_num = product_batches.index(product_batch) + 1
            logger.info(f"Starting batch {batch_num}/{len(product_batches)}")
            product_response = self._get_products_by_id(product_batch)
            self._save_response_json(product_response, batch_num)
            logger.info(f"Completed batch {batch_num}/{len(product_batches)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--config_path", required=True)
    parser.add_argument("--job_name", required=True)
    parser.add_argument("--task_name", required=True)
    args = parser.parse_args()

    configs = load_configs(
        config_file_name="config",
        config_path_str=args.config_path,
        profile=args.profile,
    )
    job_root = configs[args.job_name]
    logger.info(f"Running {args.job_name}-{args.task_name}")

    task_config = job_root.get(args.task_name, {})
    auth_conf = task_config.get("authentication", {})
    extractor = ProductsExtractor(task_config, auth=BearerAuth(auth_conf))
    extractor.run()
