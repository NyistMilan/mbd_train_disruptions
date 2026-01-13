# MBD Train Disruptions

This is the final assignment for the UTwente cource Managing Big Data proposed by group 17.

## Research Questions

What factors drive delays in the Dutch railway system, and to what extent can these disruption causes be better understood by integrating external data (e.g., weather)?

- How strongly do weather conditions (rain, wind, temperature, etc.) correlate with delay frequency?

- How do delay patterns vary across train types, providers, or neighboring countries?

- How do delays differ across time (rush hours, seasons, years)?


## Datasets Used

- [Dutch railway network & disruptions dataset](https://www.rijdendetreinen.nl/en/open-data)

## Usage

1. Clone this repository:

```
git clone https://github.com/NyistMilan/mbd_train_disruptions.git
    
cd mbd_train_disruptions
```

2. Create and activate an environment:

 ```
conda create -n mbd_train_disruptions -y

conda activate mbd_train_disruptions
```

3. Install dependencies:

```
pip install -e .
```

4. Running scripts:

scrape_train_data.py:

Note: If HDFS_DIR is present, it will move the files to HDFS.
```
python src/create_master_data.py
```

create_master_data.py:
```
spark-submit --deploy-mode cluster --driver-memory 6g --executor-memory 6g --conf spark.dynamicAllocation.maxExecutors=15 src/analyze_weather_delays.py
```

## ER Diagram

![raw_layer](imgs/raw_er.png)

## Weather Data Structure [^1]

| Parameter | Description | Unit | Observed property | Comment |
|-----------|-------------|------|-------------------|---------|
| DD | Past 10 minute mean wind direction, representative for 10 meters, in degrees; 360=north; 90=east; 180=south; 270=west; 0=calm; 990=variable | ° | Wind from direction | |
| DR | Hourly precipitation duration, in hours | h | Duration of precipitation | |
| EE | Hourly mean vapor pressure, in hectopascal | hPa | Water vapor partial pressure in air | |
| FF | Past 10 minute mean wind speed, representative for 10 meters, in meters per second | m/s | Wind speed | |
| FH | Hourly mean wind speed, representative for 10 meters, in meters per second | m/s | Wind speed | |
| FX | Hourly maximum wind gust, representative for 10 meters, in meters per second | m/s | Wind speed of gust | |
| IX | Hourly indicator present weather code; 1=manned and recorded (using code from visual observations); 2; 3=manned and omitted (no significant weather phenomenon to report; not available); 4=automatically recorded (using code from visual observations); 5; 6=automatically omitted (no significant weather phenomenon to report; not available); 7=automatically set (using code from automated observations) | 一 | Indicator present weather code | |
| N | Past 10 minute cloud cover, in okta; 9=sky invisible | okta | Cloud area fraction | |
| P | Past 1 minute mean air pressure at sea level | hPa | Air pressure at mean sea level | |
| Q | Hourly global solar radiation, in joules per square centimeter | J/cm² | Integral wrt time of surface downwelling shortwave flux in air | |
| RH | Hourly precipitation amount, in millimeters | mm | Precipitation amount | *1 |
| SQ | Hourly sunshine duration, calculated from global solar radiation, in hours | h | Duration of sunshine | *2 |
| T | Past 1 minute air temperature at 1.50 meters, in degrees Celsius | °C | Air temperature | |
| T10N | Past 6 hour minimum air temperature at 10 centimeters, in degrees Celsius | °C | Air temperature | |
| TD | Past 1 minute dew point temperature at 1.50 meters, in degrees Celsius | °C | Dew point temperature | |
| U | Past 1 minute relative atmospheric humidity at the time of observation, at 1.50 meters, as percentage | % | Relative humidity | |
| VV | Past 10 minute horizontal visibility; 0=less than 100m; 1=100-200m; 2=200-300m;...; 49=4900-5000m; 50=5-6km; 56=6-7km; 57=7-8km; ...; 79=29-30km; 80=30-35km; 81=35-40km;...; 89=more than 70km | 一 | Visibility in air | |
| W1 | Hourly indicator of fog; 0=no occurrence; 1=occurred | 一 | Fog | |
| W2 | Hourly indicator of rainfall; 0=no occurrence; 1=occurred | 一 | Rain | |
| W3 | Hourly indicator of snow. 0=no occurrence; 1=occurred | 一 | Snow | |
| W5 | Hourly indicator of thunder. 0=no occurrence; 1=occurred | 一 | Thunder | |
| W6 | Hourly indicator of ice formation. 0=no occurrence; 1=occurred | 一 | Ice formation | |
| WW | Hourly present weather code, aggregated with an algorithm using 10-minute ww (WaWa) values from the preceding 6 hours; WMO table 4680 | 一 | Present weather | |

[^1]: https://english.knmidata.nl/open-data/hourly-daily-monthly-and-annual-in-situ-meteorological-observations