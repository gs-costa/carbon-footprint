from typing import Generator

from src.common.checkpoint import Checkpoint
from src.common.etl.functions.range_years import range_years
from src.common.logger import Logger
from src.raw.global_footprint_network.endpoints import GlobalFootprintNetworkEndpoints
from src.repositories.aws.s3 import S3Repository


class GlobalFootprintNetworkRaw(GlobalFootprintNetworkEndpoints):
    """ETL for the Global Footprint Network API."""

    logger = Logger(__name__)

    def __init__(self):
        super().__init__(self)
        self.ingestion_errors: list[str] = []
        self.checkpoint = Checkpoint(bucket_name=self.environment.RAW_PATH)
        self.s3_repository = S3Repository(bucket_name=self.environment.RAW_PATH)

    def get_countries_codes(self, countries: list[dict]) -> list[dict]:
        """Create a sorted list of countries codes from the countries list. Exclude "all" country code."""
        list_countries_codes = [
            country["countryCode"] for country in countries if country["countryCode"] != self.CODE_COUNTRY_ALL
        ]
        return sorted(list_countries_codes)

    def __is_year_valide(self, year: int, valide_years: list[int]) -> bool:
        if year not in valide_years:
            self.logger.warning(f"Year {year} not found in valid years")
            self.ingestion_errors.append(f"Year {year} not found in valid years")
            return False
        return True

    def __go_next_year(self, year: int, valide_years: list[int], checkpoint: dict) -> bool:
        year_valide = self.__is_year_valide(year=year, valide_years=valide_years)
        resume_checkpoint = self.__resume_from_checkpoint(
            year=year, country_code=self.CODE_COUNTRY_ALL, checkpoint=checkpoint
        )
        return not (year_valide) or resume_checkpoint

    def __resume_from_checkpoint(self, year: int, country_code: str, checkpoint: dict) -> bool:
        """Check if the pipeline should resume from the checkpoint."""
        if self.environment.CHECKPOINT and checkpoint:
            checkpoint_year = checkpoint["year"]
            checkpoint_country = checkpoint["country_code"]
            if checkpoint_year > year or (checkpoint_year == year and checkpoint_country >= country_code):
                return True
        return False

    def get_footprint_data(
        self, countries_codes: list, start_year: int, end_year: int
    ) -> Generator[tuple[int, str, list[dict]], None, None]:
        """Get data from the Global Footprint Network API."""

        years = range_years(start_year, end_year)
        valide_years = self.get_years()
        checkpoint = self.checkpoint.get_checkpoint()
        if self.environment.CHECKPOINT and checkpoint:
            self.logger.info(f"Resuming from checkpoint: {checkpoint}")

        for year in years:
            if self.__go_next_year(year=year, valide_years=valide_years, checkpoint=checkpoint):
                continue

            if all_countries_data := self.get_all_countries_data(year=year):
                yield year, self.CODE_COUNTRY_ALL, all_countries_data
                continue

            for country_code in countries_codes:
                if self.__resume_from_checkpoint(year=year, country_code=country_code, checkpoint=checkpoint):
                    continue

                try:
                    data = self.get_data(country_code=country_code, year=year)
                    yield year, country_code, data
                except Exception as e:
                    self.ingestion_errors.append(f"Error getting data for {country_code} in {year}: {e}")

    def __load_ingestion_errors(self) -> None:
        """Ingestion errors into the S3 repository."""
        if self.ingestion_errors:
            self.s3_repository.upload_file(
                file_path=f"{self.DATA}/ingestion_errors/{self.NOW}.{self.FILE_FORMAT}", data=self.ingestion_errors
            )

    def execute(self) -> None:
        """Execute the pipeline."""
        countries = self.get_countries()
        countries_codes = self.get_countries_codes(countries=countries)

        last_successful_year = None
        last_successful_country = None

        try:
            for year, country_code, data in self.get_footprint_data(
                countries_codes=countries_codes,
                start_year=self.environment.START_YEAR,
                end_year=self.environment.END_YEAR,
            ):
                file_path = f"{self.DATA}/{year}/{country_code}.{self.FILE_FORMAT}"
                self.s3_repository.upload_file(file_path=file_path, data=data)

                last_successful_year = year
                last_successful_country = country_code

            self.logger.info("Data ingestion completed successfully.")

        finally:
            if last_successful_year and last_successful_country:
                self.checkpoint.save_checkpoint(year=last_successful_year, country_code=last_successful_country)

        self.__load_ingestion_errors()


if __name__ == "__main__":
    client = GlobalFootprintNetworkRaw()
    client.execute()
