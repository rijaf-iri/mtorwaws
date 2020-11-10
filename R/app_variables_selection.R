
variables.select <- c("RR", "Total Precipitation",
                      "TAVG", "Average Air Temperature",
                      "TMAX", "Maximum Air Temperature",
                      "TMIN", "Minimum Air Temperature",
                      "RHAVG", "Average Relative Humidity",
                      "RHMAX", "Maximum Relative Humidity",
                      "RHMIN", "Minimum Relative Humidity",
                      "RADAVG", "Average Global Radiation",
                      "RADMAX", "Maximum Global Radiation",
                      "RADMIN", "Minimum Global Radiation",
                      "PRESAVG", "Average Air Pressure",
                      "PRESMAX", "Maximum Air Pressure",
                      "PRESMIN", "Minimum Air Pressure",
                      "FFAVG", "Average Wind Speed",
                      "FFMAX", "Maximum Wind Speed",
                      "FFMIN", "Minimum Wind Speed")

variables.select <- matrix(variables.select, ncol = 2, byrow = TRUE)

variables.aws <- c('RR', 'Precipitation',
                   'RH', 'Relative Humidity',
                   'TT', 'Air Temperature',
                   'TT2', 'Air Temperature at 2m',
                   'TT10', 'Air Temperature at 10m',
                   'DD', 'Wind direction',
                   'FF', 'Wind speed',
                   'PRES', 'Pressure',
                   'RAD', 'Global solar radiation',
                   'TTg', 'Ground Temperature',
                   'TTs', 'Soil/Water Temperature',
                   'TTs10', 'Soil Temperature at 10cm',
                   'TTs20', 'Soil Temperature at 20cm',
                   'SOILM', 'Soil Moisture',
                   'EVP', 'Evaporation',
                   'WLV', 'Water Level',
                   'POWER', 'Battery Voltage/Power Supply')
variables.aws <- matrix(variables.aws, ncol = 2, byrow = TRUE)
