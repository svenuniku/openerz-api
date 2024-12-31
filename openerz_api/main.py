"""A simple connector to interact with OpenERZ API."""

import asyncio
import datetime
import logging

from aiohttp import ClientSession, ClientTimeout
from aiohttp.client_exceptions import ClientError, ContentTypeError

DEFAULT_TIMEOUT = 30


class OpenERZConnector:
    """A simple connector to interact with OpenERZ API."""

    def __init__(
        self,
        region: str,
        area: str,
        waste_types: list[str],
        session: ClientSession | None = None,
    ) -> None:
        """Initialize the API connector.

        Args:
        region (str): region/city of interest
        area (str): post code / area of interest
        waste_types (str): type of waste to be picked up (paper/cardboard/waste/cargotram/etram/organic/textile)
        session (ClientSession): aiohttp session

        """
        self.region = region
        self.area = area
        self.waste_types = waste_types
        self.start_date = datetime.datetime.now()
        self.end_date = None
        self._session = session
        self.logger = logging.getLogger(__name__)

    def _update_start_date(self):
        """Set the start day to today."""

        self.start_date = datetime.datetime.now()

    def _find_end_date(self, day_offset=31):
        """Find the end date for the request, given an offset expressed in days.

        Args:
        day_offset (int): difference in days between start and end date of the request

        """

        self.end_date = self.start_date + datetime.timedelta(days=day_offset)

    async def _make_api_request(self, waste_type: str) -> list[dict[str, any]]:
        """Construct a request and send it to the OpenERZ API."""

        headers = {"accept": "application/json"}

        start_date = self.start_date.strftime("%Y-%m-%d")
        end_date = self.end_date.strftime("%Y-%m-%d")

        payload = {
            "region": self.region,
            "area": self.area,
            "types": waste_type,
            "start": start_date,
            "end": end_date,
            "offset": 0,
            "limit": 1,
            "lang": "en",
            "sort": "date",
        }
        payload = dict(filter(lambda item: item[1] is not None, payload.items()))
        url = f"https://openerz.metaodi.ch/api/calendar.json"
        self.logger.warning(f"Query waste date with payload {payload}.")

        if use_running_session := self._session and not self._session.closed:
            session = self._session
        else:
            session = ClientSession(timeout=ClientTimeout(total=DEFAULT_TIMEOUT))

        try:
            async with self._session.get(url, params=payload, headers=headers) as r:
                if not r.ok:
                    self.logger.warning(
                        "Last request to OpenERZ was not successful. Status code: %d",
                        r.status_code,
                    )
                result = await r.json()
        finally:
            if not use_running_session:
                await session.close()
        return result
        # try:
        #    return requests.get(url, params=payload, headers=headers)
        # except requests.exceptions.RequestException as connection_error:
        #    self.logger.error(
        #        "RequestException while making request to OpenERZ: %s", connection_error
        #    )

    def _parse_api_response(self, response, waste_type: str) -> dict[str, str]:
        """Parse the JSON response received from the OpenERZ API and return a date of the next pickup."""

        response_json = response
        if response_json["_metadata"]["total_count"] == 0:
            self.logger.warning("Request to OpenERZ returned no results.")
            return None
        result_list = response_json.get("result")
        # replace all empty value-strings with None
        first_scheduled_pickup = {
            k: (None if v == "" else v) for k, v in result_list[0].items()
        }
        if (
            first_scheduled_pickup["region"] == self.region
            and first_scheduled_pickup["area"] == self.area
            and first_scheduled_pickup["waste_type"] == waste_type
        ):
            return {
                k: v
                for k, v in first_scheduled_pickup.items()
                if k in ["date", "station", "description"]
            }
        self.logger.warning(
            "Either region, area or waste type does not match the ones specified in the configuration."
        )
        return None

    async def find_next_pickup(
        self, waste_type: str, day_offset: int = 31
    ) -> dict[str, str]:
        """Find the next pickup date within the next X days, given zip_code and waste type.

        Args:
        waste_type (str): type of waste to be picked up (paper/cardboard/waste/cargotram/etram/organic/textile)
        day_offset (int): difference in days between start and end date of the request

        """

        self._update_start_date()
        self._find_end_date(day_offset=day_offset)
        response = await self._make_api_request(waste_type)
        return self._parse_api_response(response, waste_type)


class OpenERZParameters:
    """A simple connector to get possible parameters for OpenERZ API."""

    def __init__(self, session: ClientSession | None = None) -> None:
        """Initialize the API Parameter connector.

        Args:
        session (ClientSession): aiohttp session

        """
        self._session = session
        self.logger = logging.getLogger(__name__)

    async def _query_parameters(
        self, param: str, payload: dict[str, str] | None = None
    ) -> list[str]:
        """Query Api parameters from OpenERZ API."""
        valid_params = ["areas", "regions", "types"]
        if param not in valid_params:
            raise ValueError(
                f"Parameter '{param}' is not valid. Valid parameters are: {valid_params}.",
            )

        headers = {"accept": "application/json"}

        url = f"https://openerz.metaodi.ch/api/parameter/{param}"
        self.logger.debug(f"Query {param} with payload {payload}.")

        if use_running_session := self._session and not self._session.closed:
            session = self._session
        else:
            session = ClientSession(timeout=ClientTimeout(total=DEFAULT_TIMEOUT))

        try:
            async with self._session.get(url, params=payload, headers=headers) as r:
                if not r.ok:
                    self.logger.warning(
                        "Last request to OpenERZ was not successful. Status code: %d",
                        r.status_code,
                    )
                result = await r.json()
        finally:
            if not use_running_session:
                await session.close()

        self.logger.debug(f"\tResult: {result['result']}.")
        return result["result"]

    async def get_regions(self) -> list[str]:
        """Query regions from OpenERZ API."""
        return await self._query_parameters(param="regions")

    async def get_areas(self, region: str) -> list[str]:
        """Query areas for single region from OpenERZ API."""
        areas = await self._query_parameters(param="areas", payload={"region": region})
        # get list of areas for <region>
        return [d["area"] for d in areas if d["region"] == region]

    async def get_types(self, region: str) -> list[str]:
        """Query types from OpenERZ API."""
        return await self._query_parameters(param="types", payload={"region": region})
