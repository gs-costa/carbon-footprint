from src.common.schema.base import BaseSchema, Column


class GlobalFootprintNetworkSchema(BaseSchema):
    columns = [
        Column(name="year", column_type="int64", merge_key=True),
        Column(name="country_code", column_type="int64", rename="countryCode", merge_key=True),
        Column(name="country_name", column_type="object", rename="countryName"),
        Column(name="short_name", column_type="object", rename="shortName"),
        Column(name="isoa2", column_type="object"),
        Column(name="record", column_type="object", merge_key=True),
        Column(name="crop_land", column_type="float64", rename="cropLand"),
        Column(name="grazing_land", column_type="float64", rename="grazingLand"),
        Column(name="forest_land", column_type="float64", rename="forestLand"),
        Column(name="fishing_ground", column_type="float64", rename="fishingGround"),
        Column(name="builtup_land", column_type="float64", rename="builtupLand"),
        Column(name="carbon", column_type="float64"),
        Column(name="value", column_type="float64"),
        Column(name="score", column_type="object"),
    ]
