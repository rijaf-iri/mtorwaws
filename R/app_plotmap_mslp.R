
#' Compute hourly mean sea level pressure.
#'
#' Compute hourly mean sea level pressure data to display on map.
#' 
#' @param time the time to display in the format "YYYY-MM-DD-HH"
#' @param aws_dir full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

dispMapMSLPHourly <- function(time, aws_dir){
    spdon <- compute_mslp(time, aws_dir)
    if(spdon$status == "no-data") return(convJSON(spdon))
    mslp <- spdon$data$MSLP

    ops <- list(var.name = "MSL", colorP = 'rainbow')
    pars <- do.call(defColorKeyOptions, ops)

    zmin <- suppressWarnings(min(mslp, na.rm = TRUE))
    if(!is.infinite(zmin)){
        pars$breaks[1] <- ifelse(pars$breaks[1] > zmin, zmin, pars$breaks[1])
    }
    zmax <- suppressWarnings(max(mslp, na.rm = TRUE))
    if(!is.infinite(zmax)){
        nl <- length(pars$breaks)
        pars$breaks[nl] <- ifelse(pars$breaks[nl] < zmax, zmax, pars$breaks[nl])
    }

    spdon$color <- pars$colors[findInterval(mslp, pars$breaks, rightmost.closed = TRUE, left.open = TRUE)]

    nom <- gsub('\\.', '', names(pars))
    names(pars) <- nom

    spdon$key <- list(labels = pars$legendaxis$labels, colors = pars$colors)

    return(convJSON(spdon))
}

compute_mslp <- function(time, aws_dir){
    spdon <- spatialAggrAWS("hourly", time, aws_dir)
    if(spdon$status == "no-data") return(spdon)

    spdon$data <- spdon$data[c('id', 'stationName', 'longitude', 'latitude',
                               'elevation', 'AWSGroup', 'TAVG', 'PRESAVG')]
    spdon$data[["PRESAVG"]] <- ifelse(spdon$data[["PRESAVG"]] == 0, NA, spdon$data[["PRESAVG"]])
    mslp <- mslp_corrections(spdon$data[["PRESAVG"]], spdon$data[["TAVG"]],
                                spdon$data[["elevation"]], spdon$data[["latitude"]])
    spdon$data$MSLP <- mslp

    return(spdon)
}

mslp_corrections <- function(pres, temp, alti, lat){
    # constants
    # barometric pressure of the air column (hPa)
    b <- 1013.25
    # barometrics constant (m)
    K <- 18400.0
    # coefficient of thermal expansion of the air
    a <- 0.0037
    # constant depending on the figure of the earth
    k <- 0.0026
    # lr is calculated assuming a temperature gradient of 0.5 degC/100 metres. (C/m)
    lr <- 0.005
    # radius of the earth (m)
    R <- 6367324

    # convert latitude  to radians
    phi <- lat * pi / 180
    # delta: altitude difference. Sea level 0m
    dZ <- alti - 0
    # average temperature of air column
    at = temp + (lr * dZ) / 2;
    # vapor pressure [hPa]
    e <- 10^(7.5 * at / (237.3 + at)) * 6.1078
    # correction for atmospheric temperature
    corT <- 1 + a * at
    # correction for humidity
    corH <- 1 / (1 - 0.378 * (e / b))
    # correction for asphericity of earth (latitude)
    corE <- 1 / (1 - (k * cos(2 * phi)))
    # correction for variation of gravity with height
    corG <- 1 + alti / R

    msl <- dZ / (K * corT * corH * corE * corG)
    mslp <- pres * 10^msl

    return(round(mslp, 2))
}
