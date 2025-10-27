from datetime import datetime

from src.common.clients.base_http import BaseHTTPClient
from src.common.environment import Environment
from src.common.logger import Logger
from src.common.repositories.aws.s3 import S3Repository


class GlobalFootprintNetworkEndpoints(BaseHTTPClient):
    """Endpoints for the Global Footprint Network API."""

    logger = Logger(__name__)
    BASE_URL = "https://api.footprintnetwork.org/v1"
    COUNTRIES = "countries"
    FILE_FORMAT = "json"
    YEARS = "years"
    DATA = "data"
    NOW = datetime.now()
    CODE_COUNTRY_ALL = "all"

    default_headers = {"HTTP_ACCEPT": "application/json"}

    def __init__(self):
        super().__init__(self.BASE_URL, default_headers=self.default_headers)
        self.environment = Environment()
        self.auth_tuple = (self.environment.API_USERNAME, self.environment.API_KEY)
        self.s3_repository = S3Repository(bucket_name=self.environment.RAW_PATH)

    def get_countries(self) -> list[dict]:
        """Get countries from the Global Footprint Network API."""
        if self.environment.UPDATE_COUNTRIES:
            countries = self.get(self.COUNTRIES, auth=self.auth_tuple)
            self.s3_repository.upload_file(
                file_path=f"{self.COUNTRIES}/{self.COUNTRIES}.{self.FILE_FORMAT}", data=countries
            )
        else:
            countries = self.s3_repository.get_file(file_path=f"{self.COUNTRIES}/{self.COUNTRIES}.{self.FILE_FORMAT}")
        return countries

    def get_years(self) -> list[int]:
        """Get years from the Global Footprint Network API."""
        try:
            years = self.get(self.YEARS, auth=self.auth_tuple)
            list_years = [int(year["year"]) for year in years]
            return list_years
        except Exception as e:
            self.logger.error(f"Error getting years: {e}")
            raise e

    def get_all_countries_data(self, year: int) -> list[dict]:
        """Get data for all countries for a given year."""
        try:
            return self.get(f"{self.DATA}/{self.CODE_COUNTRY_ALL}/{year}", auth=self.auth_tuple)
        except Exception as e:
            self.logger.error(f"Error getting data for all countries for {year}: {e}")
            return []

    def get_data(self, country_code: str, year: int) -> list[dict]:
        """Get data for a given country and year."""
        return self.get(f"{self.DATA}/{country_code}/{year}", auth=self.auth_tuple)
