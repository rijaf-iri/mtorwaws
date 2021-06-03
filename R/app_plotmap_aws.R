
#' Get AWS 10 minutes spatial data.
#'
#' Get AWS 10 minutes spatial data to display on map.
#' 
#' @param time the time to display in the format "YYYY-MM-DD-HH-MM".
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

displayMAP10min <- function(time, aws_dir){
    spdon <- spatial10minAWS(time, aws_dir)
    if(spdon$status == "no-data") return(convJSON(spdon))
    don <- spdon$data

    ############

    varP <- c("PRECIP", "TMIN", "TMAX", "TAVG", "RHMIN", "RHMAX",
              "RHAVG","RADMIN", "RADMAX", "RADAVG",
              "PMIN", "PMAX", "PAVG", "WGFF", "WFF")
    outKey <- list()

    donP <- lapply(varP, function(x){
        if(x == "PRECIP"){
            colorC <- RColorBrewer::brewer.pal(n = 9, name = "YlGnBu")
            ops <- list(var.name = "RR", customC = TRUE, colorC = colorC)
            ix <- !is.na(don[[x]]) & don[[x]] == 0
            vr <- 'RR'
        }
        if(x %in% c("TMIN", "TMAX", "TAVG")){
            ops <- list(var.name = "TT")
            ix <- NULL
            vr <- 'TT'
        }
        if(x %in% c("RHMIN", "RHMAX", "RHAVG")){
            colorC <- c('orange', 'yellow', 'chartreuse', 'green', 'darkgreen')
            ops <- list(var.name = "RH", customC = TRUE, colorC = colorC)
            ix <- NULL
            vr <- 'RH'
        }
        if(x %in% c("RADMIN", "RADMAX", "RADAVG")){
            colorC <- c('#37a39a', '#4dbf99', '#69d681', '#95eb5b', '#baf249',
                        '#e0f545', '#fcf33f', '#fae334', '#f7cf2d', '#f5b222',
                        '#ed8e1a', '#eb6117', '#d6361a', '#b01729')
            ops <- list(var.name = "RG", customC = TRUE, colorC = colorC)
            ix <- !is.na(don[[x]]) & don[[x]] == 0
            vr <- 'RG'
        }
        if(x %in% c("PMIN", "PMAX", "PAVG")){
            ops <- list(var.name = "PR", colorP = 'rainbow')
            ix <- NULL
            vr <- 'PR'
        }
        if(x %in% c("WGFF", "WFF")){
            colorC <- RColorBrewer::brewer.pal(n = 9, name = "YlOrBr")
            ops <- list(var.name = "FF", customC = TRUE, colorC = colorC)
            ix <- !is.na(don[[x]]) & don[[x]] == 0
            vr <- 'FF'
        }

        pars <- do.call(defColorKeyOptions, ops)

        ###############
        zmin <- suppressWarnings(min(don[[x]], na.rm = TRUE))
        if(!is.infinite(zmin)){
            pars$breaks[1] <- ifelse(pars$breaks[1] > zmin, zmin, pars$breaks[1])
        }
        zmax <- suppressWarnings(max(don[[x]], na.rm = TRUE))
        if(!is.infinite(zmax)){
            nl <- length(pars$breaks)
            pars$breaks[nl] <- ifelse(pars$breaks[nl] < zmax, zmax, pars$breaks[nl])
        }

        kolor.p <- pars$colors[findInterval(don[[x]], pars$breaks, rightmost.closed = TRUE, left.open = TRUE)]
        if(!is.null(ix)) kolor.p[ix] <- "#FFFFFF" # "transparent"
 
        ###############
        nom <- gsub('\\.', '', names(pars))
        names(pars) <- nom

        ##########
        pars <- list(labels = pars$legendaxis$labels, colors = pars$colors)
        ##########

        if(is.null(outKey[[vr]])) outKey[[vr]] <<- pars

        ###############
        return(kolor.p)
    })

    names(donP) <- varP

    ############
    don <- list(date = spdon$date, data = don, color = donP,
                key = outKey, status = spdon$status)

    return(convJSON(don))
}

############
#' Get AWS aggregated spatial data.
#'
#' Get AWS aggregated spatial data to display on map.
#' 
#' @param tstep the time step of the data.
#' @param time the time to display in the format,
#'              hourly: "YYYY-MM-DD-HH",
#'              daily: "YYYY-MM-DD",
#'              pentad: "YYYY-MM-DD",
#'              dekadal: "YYYY-MM-DD",
#'              monthly: "YYYY-MM"
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

displayMAPAggr <- function(tstep, time, aws_dir){
    spdon <- spatialAggrAWS(tstep, time, aws_dir)
    if(spdon$status == "no-data") return(convJSON(spdon))
    don <- spdon$data

    ############

    varP <- c("PRECIP", "TMIN", "TMAX", "TAVG", "RHMIN", "RHMAX",
              "RHAVG","RADMIN", "RADMAX", "RADAVG", "PRESMIN",
              "PRESMAX", "PRESAVG", "FFMIN", "FFMAX", "FFAVG")
    outKey <- list()

    donP <- lapply(varP, function(x){
        if(x == "PRECIP"){
            colorC <- RColorBrewer::brewer.pal(n = 9, name = "YlGnBu")
            ops <- list(var.name = "RR", timestep = tstep, customC = TRUE, colorC = colorC)
            ix <- !is.na(don[[x]]) & don[[x]] == 0
            vr <- 'RR'
        }
        if(x %in% c("TMIN", "TMAX", "TAVG")){
            ops <- list(var.name = "TT")
            ix <- NULL
            vr <- 'TT'
        }
        if(x %in% c("RHMIN", "RHMAX", "RHAVG")){
            colorC <- c('orange', 'yellow', 'chartreuse', 'green', 'darkgreen')
            ops <- list(var.name = "RH", customC = TRUE, colorC = colorC)
            ix <- NULL
            vr <- 'RH'
        }
        if(x %in% c("RADMIN", "RADMAX", "RADAVG")){
            colorC <- c('#37a39a', '#4dbf99', '#69d681', '#95eb5b', '#baf249',
                        '#e0f545', '#fcf33f', '#fae334', '#f7cf2d', '#f5b222',
                        '#ed8e1a', '#eb6117', '#d6361a', '#b01729')
            ops <- list(var.name = "RG", customC = TRUE, colorC = colorC)
            ix <- !is.na(don[[x]]) & don[[x]] == 0
            vr <- 'RG'
        }
        if(x %in% c("PRESMIN", "PRESMAX", "PRESAVG")){
            ops <- list(var.name = "PR", colorP = 'rainbow')
            ix <- NULL
            vr <- 'PR'
        }
        if(x %in% c("FFMIN", "FFMAX", "FFAVG")){
            colorC <- RColorBrewer::brewer.pal(n = 9, name = "YlOrBr")
            ops <- list(var.name = "FF", customC = TRUE, colorC = colorC)
            ix <- !is.na(don[[x]]) & don[[x]] == 0
            vr <- 'FF'
        }

        pars <- do.call(defColorKeyOptions, ops)

        ###############
        zmin <- suppressWarnings(min(don[[x]], na.rm = TRUE))
        if(!is.infinite(zmin)){
            pars$breaks[1] <- ifelse(pars$breaks[1] > zmin, zmin, pars$breaks[1])
        }
        zmax <- suppressWarnings(max(don[[x]], na.rm = TRUE))
        if(!is.infinite(zmax)){
            nl <- length(pars$breaks)
            pars$breaks[nl] <- ifelse(pars$breaks[nl] < zmax, zmax, pars$breaks[nl])
        }

        kolor.p <- pars$colors[findInterval(don[[x]], pars$breaks, rightmost.closed = TRUE, left.open = TRUE)]
        if(!is.null(ix)) kolor.p[ix] <- "#FFFFFF" # "transparent"
 
        ###############
        nom <- gsub('\\.', '', names(pars))
        names(pars) <- nom

        ##########
        pars <- list(labels = pars$legendaxis$labels, colors = pars$colors)
        ##########

        if(is.null(outKey[[vr]])) outKey[[vr]] <<- pars

        ###############
        return(kolor.p)
    })

    names(donP) <- varP

   ############
    don <- list(date = spdon$date, data = don, color = donP,
                key = outKey, status = spdon$status)

    return(convJSON(don))
}

################

spatial10minAWS <- function(time, aws_dir){
    daty <- strptime(time, "%Y-%m-%d-%H-%M", tz = "Africa/Kigali")
    dout <- as.character(daty)
    daty <- format(daty, "%Y%m%d%H%M")

    timefile <- file.path(aws_dir, "PROC", "SPATIAL", "Minutes", paste0(daty, ".rds"))

    data.null <- list(date = dout, data = "null", status = "no-data")
    if(!file.exists(timefile)) return(data.null)

    ## check and Read QC data

    don <- readRDS(timefile)
    if(is.null(don$data)) return(data.null)

    coordAWS <- readCoordsAWS(aws_dir)
    ix <- match(don$id, coordAWS$id)
    nom <- c("id", "stationName", "longitude", "latitude", "elevation", "AWSGroup")
    coordAWS <- coordAWS[ix, nom, drop = FALSE]
    if(nrow(coordAWS) == 0) return(data.null)

    coordAWS$longitude <- as.numeric(as.character(coordAWS$longitude))
    coordAWS$latitude <- as.numeric(as.character(coordAWS$latitude))
    coordAWS$elevation <- as.numeric(as.character(coordAWS$elevation))

    ret <- lapply(don$data, function(x){
        tmp <- list(
                    PRECIP = x[['RR']][['Tot']],
                    TMIN = x[['TT']][['Min']],
                    TMAX = x[['TT']][['Max']],
                    TAVG = x[['TT']][['Ave']],
                    RHMIN = x[['RH']][['Min']],
                    RHMAX = x[['RH']][['Max']],
                    RHAVG = x[['RH']][['Ave']],
                    RADMIN = x[['RAD']][['Min']],
                    RADMAX = x[['RAD']][['Max']],
                    RADAVG = x[['RAD']][['Ave']],
                    PMIN = x[['PRES']][['Min']],
                    PMAX = x[['PRES']][['Max']],
                    PAVG = x[['PRES']][['Ave']],
                    WDD = local({
                                d1 <- x[['DD']][['Ave']]
                                d2 <- x[['DD']][['PrevDir']]
                                if(is.null(d1) & !is.null(d2)) d1 <- d2
                                d1
                              }),
                    WGFF = x[['FF']][['Max']],
                    WFF = x[['FF']][['Ave']]
                  )
        inull <- sapply(tmp, is.null)
        tmp[inull] <- NA
        as.data.frame(tmp)
    })
    ret <- do.call(rbind.data.frame, ret)
    don <- data.frame(ret, coordAWS, stringsAsFactors = FALSE)
    don <- don[!is.na(don$longitude) & !is.na(don$latitude), , drop = FALSE]

    if(nrow(don) == 0) return(data.null)

    list(date = dout, data = don, status = "ok")
}

spatialAggrAWS <- function(tstep, time, aws_dir){
    infoData <- switch(tstep,
                       'hourly' = local({
                           dirA <- "Hourly"
                           tt <- strptime(time, "%Y-%m-%d-%H", tz = "Africa/Kigali")
                           tt1 <- format(tt, "%Y%m%d%H")
                           list(time = tt, date = tt1, dir = dirA)
                       }),
                       'daily' = local({
                           dirA <- "Daily"
                           tt <- as.Date(time, "%Y-%m-%d")
                           tt1 <- format(tt, "%Y%m%d")
                           list(time = tt, date = tt1, dir = dirA)
                       }),
                       'pentad' = local({
                           dirA <- "Pentadal"
                           tt <- as.Date(time, "%Y-%m-%d")
                           tmp <- as.numeric(format(tt, '%d'))
                           tt1 <- paste0(format(tt, "%Y%m"), tmp)
                           list(time = tt, date = tt1, dir = dirA)
                       }),
                       'dekadal' = local({
                           dirA <- "Dekadal"
                           tt <- as.Date(time, "%Y-%m-%d")
                           tmp <- as.numeric(format(tt, '%d'))
                           tt1 <- paste0(format(tt, "%Y%m"), tmp)
                           list(time = tt, date = tt1, dir = dirA)
                       }),
                       'monthly' = local({
                           dirA <- "Monthly"
                           tt <- as.Date(time, "%Y-%m-%d")
                           tt1 <- format(tt, "%Y%m")
                           list(time = tt, date = tt1, dir = dirA)
                       })
                     )

    timefile <- file.path(aws_dir, "PROC", "SPATIAL", infoData$dir, paste0(infoData$date, ".rds"))
    dout <- as.character(infoData$time)

    data.null <- list(date = dout, data = "null", status = "no-data")
    if(!file.exists(timefile)) return(data.null)

    don <- readRDS(timefile)
    if(is.null(don$data)) return(data.null)

    coordAWS <- readCoordsAWS(aws_dir)
    ix <- match(don$id, coordAWS$id)
    nom <- c("id", "stationName", "longitude", "latitude", "elevation", "AWSGroup")
    coordAWS <- coordAWS[ix, nom, drop = FALSE]
    if(nrow(coordAWS) == 0) return(data.null)
 
    coordAWS$longitude <- as.numeric(as.character(coordAWS$longitude))
    coordAWS$latitude <- as.numeric(as.character(coordAWS$latitude))
    coordAWS$elevation <- as.numeric(as.character(coordAWS$elevation))

    ret <- lapply(don$data, function(x){
        tmp <- list(
                    PRECIP = x[['RR']][['Tot']],
                    TMIN = x[['TT']][['Min']],
                    TMAX = x[['TT']][['Max']],
                    TAVG = x[['TT']][['Ave']],
                    RHMIN = x[['RH']][['Min']],
                    RHMAX = x[['RH']][['Max']],
                    RHAVG = x[['RH']][['Ave']],
                    RADMIN = x[['RAD']][['Min']],
                    RADMAX = x[['RAD']][['Max']],
                    RADAVG = x[['RAD']][['Ave']],
                    PRESMIN = x[['PRES']][['Min']],
                    PRESMAX = x[['PRES']][['Max']],
                    PRESAVG = x[['PRES']][['Ave']],
                    FFMIN = x[['FF']][['Min']],
                    FFMAX = x[['FF']][['Max']],
                    FFAVG = x[['FF']][['Ave']]
                  )
        inull <- sapply(tmp, is.null)
        tmp[inull] <- NA
        as.data.frame(tmp)
    })
    ret <- do.call(rbind.data.frame, ret)
    don <- data.frame(ret, coordAWS, stringsAsFactors = FALSE)
    don <- don[!is.na(don$longitude) & !is.na(don$latitude), , drop = FALSE]

    if(nrow(don) == 0) return(data.null)

    list(date = dout, data = don, status = "ok")
}
