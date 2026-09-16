import json
import logging
from pathlib import Path
import urllib.request

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_URL = "https://fakestoreapi.com/products"
OUTPUT_PATH = Path("data/raw_transactions/products_api.json")


def fetch_and_stage_catalog() -> None:
    """Fetches JSON catalog metadata from REST API and writes to local raw disk."""
    logger.info(f"Fetching raw catalog from HTTP endpoint: {API_URL}")
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    req = urllib.request.Request(API_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as response:
        if response.status == 200:
            payload = json.loads(response.read().decode("utf-8"))
            with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            logger.info(f"Staged {len(payload)} products to {OUTPUT_PATH}")
        else:
            raise RuntimeError(f"API request failed with status: {response.status}")


if __name__ == "__main__":
    fetch_and_stage_catalog()
