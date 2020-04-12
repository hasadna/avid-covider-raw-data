# avid-covider-raw-data

## Format of raw data 

All raw data should be uploaded to the `input/` directory.

Data for a single day should be uploaded in a single CSV file.

The file should be named according to date the data is relevant to (ISO format). For example: `2020-03-21.csv`.

The CSV file should be comma delimited, ASCII encoded and using double-quoting for quoting 'special' text.

### Columns

The file will have the following columns -

| Column Name | Explanation | Content |
|---|---|---|
| date | Date of the data | ISO date of the data (e.g. `2020-03-21`) |
| id | internal id | positive integer |
| area_id | The official id of the area | Statistic Area id (`xxxx-yy`) or CBS City id (i.e. just the `xxxx`) |
| is_city | Is this a city or a neighborhood? | Boolean, 1 for city and 0 for neighborhood |
| city_name | The English name of the city the area belongs to | Official English name as defined [here](https://data.gov.il/dataset/citiesandsettelments). Name should be in all uppercase letters |
| population | Number of people living in the area | non-zero positive integer |
| num_reports | Number of reports in this area. | non-zero positive integer. If number is below the privacy threshold, leave empty |
| symptoms_ratio | The ratio of the symptoms for this area for this day | A positive number  |
| symptoms_ratio_confidence | The confidence for the ratio for this day | A number between 0 and 1 (using the entire range) |
| num_reports_weighted | Number of reports in this area over the last time period | non-zero positive integer. If number is below the privacy threshold, leave empty |
| symptoms_ratio_weighted | The ratio of the symptoms for this area over the last time period | A positive number  |
| symptoms_ratio_confidence_weighted | The confidence for the ratio over the last time period | A number between 0 and 1 (using the entire range) |


*Note*: 'the last time period' is a time period longer than a day which might change from time to time

## Format of GeoJSON

GeoJSON files for cities and neighborhoods should uploaded to the `geo/` directory.

File names should be `cities.geojson` and `neighborhoods.geojson` respectively.

Each polygon should only contain on property which is the `id` - the internal id conforming to the `id` column in the raw data files.