import asyncio
import json
import logging
import argparse
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set

import click as click
from aiohttp import ClientError
from aiohttp_retry import ExponentialRetry, RetryClient
from aiolimiter import AsyncLimiter
from auth import refresh_token
from bigquery import save_to_bigquery


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)


@dataclass
class Counter:
    counter: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def inc(self) -> int:
        async with self.lock:
            self.counter += 1
        return self.counter


counter = Counter()
limiter = AsyncLimiter(1800, 3600)

TOKEN_DICT = {}


def get_headers():
    headers = {"Authorization": f"Bearer {TOKEN_DICT['access_token']}"}
    return headers


def ensure_https(url: str) -> str:
    """Ensure that the URL is https, not http.
    Quora sends http URLs for pagination, but then redirects them to https. This
    causes issues with aiohttp, as it removes Authorization headers on redirect.
    Args:
        url (str): If http, it's changed to https. Otherwise, it's returned unchanged.
    Returns:
        str: The changed URL.
    """
    return url.replace("http://", "https://")


async def get_json_response(
    session: RetryClient, url: str, params: Dict[str, str]
) -> Optional[dict]:
    try:
        url = ensure_https(url)
        opt = dict(params=params, headers=get_headers())
        async with limiter, session.get(url, **opt) as response:
            response.raise_for_status()
            res = await response.json()
            if "error" in res:
                raise ValueError(f"API returned error: {res['error']}")
            return res
    except (ValueError, ClientError):
        logging.exception("Error when making request")
        return None
    finally:
        await counter.inc()


async def get_response_data(
    session: RetryClient, url: str, params: Dict[str, str]
) -> List[dict]:
    data = []
    while True:
        response = await get_json_response(session, url, params)
        if response is None:
            break
        data.extend(response.get("data", []))

        next_url = response.get("paging", {}).get("next")
        if next_url is None:
            break
        url = next_url
        params = {}
    return data


async def get_ad_data(session: RetryClient, ad_id: int) -> Optional[List[dict]]:
    data = await get_response_data(
        session,
        f"https://api.quora.com/ads/v0/ads/{ad_id}",
        {
            "conversionTypes": "Generic,AppInstall,Purchase,GenerateLead,CompleteRegistration,AddPaymentInfo,AddToCart,AddToWishlist,InitiateCheckout,Search",  # NOQA
            "granularity": "DAY",
            "fields": "accountId,accountName,accountCurrency,adId,adName,adSetId,adSetName,bidAmount,campaignId,campaignName,clicks,conversions,cpc,ctr,impressions,landingURL,spend,status",  # NOQA
            "presetTimeRange": "LAST_30_DAYS",
        },
    )
    if data:
        result = flatten_key(ad_data=data, key="conversions")
        for item in result:
            if item.get("spend", 0) > 0:
                item["spend"] = item["spend"] / 10000.0
        return result
    else:
        return None


async def get_campaign_ids(session: RetryClient) -> List[int]:
    data = await get_response_data(
        session,
        f"https://api.quora.com/ads/v0/accounts/{TOKEN_DICT['account_id']}",
        {
            "fields": "accountId,accountName,campaignId,campaignName",
            "level": "CAMPAIGN",
            "sort": "campaignId",
        },
    )
    campaign_ids = [d["campaignId"] for d in data]
    return campaign_ids


async def get_ad_ids(session: RetryClient, campaign_ids: List[int]) -> Set[int]:
    all_data = []
    for campaign_id in campaign_ids:
        data = await get_response_data(
            session,
            f"https://api.quora.com/ads/v0/campaigns/{campaign_id}",
            {
                "fields": "accountId,accountName,adId,adName",
                "level": "AD",
                "sort": "adId",
            },
        )
        all_data.extend(data)
    ad_ids = {d["adId"] for d in all_data}
    return ad_ids


def flatten_key(ad_data: List[dict], key: str, prefix: str = "") -> List[dict]:
    for item in ad_data:
        val = item.get(key)
        if val is None:
            continue
        for item_key in val:
            item[f"{prefix}{item_key}"] = val.get(item_key)
        del item[key]
    return ad_data


def to_json_file(arr: list, file_name: str) -> None:
    with open(file_name, "w") as file:
        for item in arr:
            json.dump(item, file)
            file.write("\n")


async def get_ads_data(session: RetryClient, ad_ids: Iterable[int]) -> List[dict]:
    data: List[dict] = []
    for ad_id in ad_ids:
        logging.info(f"ad_id {ad_id}")
        if ads := await get_ad_data(session, ad_id):
            data.extend(ads)
    return data


async def async_main(project_id: str, results_file: str) -> None:
    retry = ExponentialRetry(attempts=5, start_timeout=1)
    async with RetryClient(retry_options=retry) as session:
        campaign_ids = await get_campaign_ids(session)
        ad_ids = await get_ad_ids(session, campaign_ids)
        logging.info(
            f"== fetching data for {len(ad_ids)} ads {len(campaign_ids)} campaigns  =="
        )
        all_data = await get_ads_data(session, ad_ids)
    logging.info(f"{counter.counter} requests made")
    to_json_file(all_data, results_file)
    save_to_bigquery(project_id, results_file)


@click.command()
@click.option("--results-file", default="data.json", type=str)
@click.option("--project_id", default="turing-230020", type=str)
def main(project_id: str, results_file: str) -> None:
    try:
        asyncio.run(async_main(project_id, results_file))
    except ClientError as e:
        logging.error(f"HTTP error occurred: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p_id",
        "--project_id",
        dest="project_id",
        type=str,
        required=False,
        help="GCP Project ID",
    )
    args = parser.parse_args()
    TOKEN_DICT = refresh_token()
    main()
