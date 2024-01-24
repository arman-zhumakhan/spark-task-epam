import os
import zipfile

import config

weather_dir = config.WEATHER_DIR


def combine_weather_zips(weather_dir: str) -> None:
    
    weather_subzips = [f"{weather_dir}/{file_name}" for file_name in os.listdir(weather_dir)]
    output_zip = f"{weather_dir}/weather.zip"
    zipped_files = set()
    
    with zipfile.ZipFile(output_zip, "a") as weather_zip:
        for weather_subzip in weather_subzips:
            with zipfile.ZipFile(weather_subzip, 'r') as weather_subfiles:
                for file in weather_subfiles.namelist():
                    if file in zipped_files:
                        continue
                    zipped_files.add(file)
                    weather_zip.writestr(file, weather_subfiles.open(file).read())

if __name__=='__main__':
    combine_weather_zips(weather_dir)
