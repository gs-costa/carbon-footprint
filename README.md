# carbon-footprint
ETL Pipeline for carbon footprint evolution by country since 2010 based on the data provided by the Global Footprint Network.

## Decisions

- When getting carbon footprint from api, decided to separate countries update from footprint, because it is not necessary to update countries everytime
- Add a error evaluation to don't stop pipeline if request fail for 1 country in 1 date:
    - f"Error getting data for {country['countryCode']} in {year}: {e}"
- The checkpoint system allows the pipeline to be resilient to failures and can restart from the last successful state. Trigger to save checkpoint when process fails, finishes or is interrupted.
- Don't storage valide years, because every request is good to know if there are new valide years
- check if year have file for all countries, if not request countries separately
- renamed columns to snake case, enforced types and selected columns, for consistency
- converted schema to duckdb schema
- src/raw/global_footprint_network/api_request.py: request data from API (simulate Lambda 1)
- yield in api_request.py simulate SQS service, delivering messages from api_request to ingestion, 1 by 1.
- src/raw/global_footprint_network/ingestion.py: ingest data to S3 (simulate Lambda 2)

## Limitations

### Endpoint Years
Return only the number of years, not the list of countries for which data is available as specified in documentation.
If "all" countries data endpoint is not available for an year, then should request for every country and check if it is available.