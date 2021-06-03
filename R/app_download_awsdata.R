#' Get 10 minutes data.
#'
#' Get 10 minutes data for download.
#' 
#' @param aws AWS ID.
#' @param vars variables.
#' @param start start time.
#' @param end end time.
#' @param aws_net AWS network.
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return CSV format object
#' 
#' @export

downAWSMinDataCSV <- function(aws, vars, start, end, aws_net, aws_dir)
{
    dirTS <- file.path(aws_dir, "PROC", "TIMESERIES", "Minutes")
    awsfile <- file.path(dirTS, aws_net, paste0(aws, ".rds"))
    don <- readRDS(awsfile)
    daty <- strptime(don[["date"]], "%Y%m%d%H%M%S", tz = "Africa/Kigali")
    don <- don$data[[vars]]

    start <- strptime(start, "%Y-%m-%d-%H-%M", tz = "Africa/Kigali")
    end <- strptime(end, "%Y-%m-%d-%H-%M", tz = "Africa/Kigali")
    ix <- daty >= start & daty <= end

    if(any(ix)){
        daty <- format(daty, "%Y%m%d%H%M")
        nom <- names(don)
        don <- data.frame(Time = daty[ix], var = don[ix, , drop = FALSE])
        names(don) <- c('Time', nom)
    }else{
        don <- data.frame(status = "no.data")
    }

    return(convCSV(don))
}

##########
#' Get aggregated data.
#'
#' Get aggregated data displayed on the table for download.
#' 
#' @param tstep time step.
#' @param aws AWS ID.
#' @param start start date.
#' @param end end date.
#' @param aws_net AWS network.
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return CSV format object
#' 
#' @export

downTableAggrCSV <- function(tstep, aws, start, end, aws_net, aws_dir){
    don0 <- data.frame(Date = NA, Status = "no.data")
    don <- filterAWS_AggrData(tstep, aws, start, end, aws_net, aws_dir)
    if(!is.null(don)){
        don1 <- round(don$data, 1)
        daty1 <- don$date
        ina <- rowSums(!is.na(don1)) > 0
        if(any(ina)){
            don0 <- don1[ina, , drop = FALSE]
            daty0 <- daty1[ina]
            don0[is.na(don0)] <- -99
            don0 <- data.frame(Date = daty0, don0)
            rownames(don0) <- NULL
        }
    }

    return(convCSV(don0))
}

##########
#' Get aggregated data.
#'
#' Get aggregated data to download for multiple AWS.
#' 
#' @param tstep time step.
#' @param aws vector of AWS IDs.
#' @param vars variables.
#' @param pars parameters.
#' @param start start time.
#' @param end end time.
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return CSV format object
#' 
#' @export

downTableAggrDataSelCSV <- function(tstep, aws, vars, pars, start, end, aws_dir){
    coordAWS <- readCoordsAWS(aws_dir)
    iaws <- match(aws, coordAWS$id)
    coordAWS <- as.list(coordAWS[iaws, , drop = FALSE])
    aws_net <- coordAWS$AWSGroup
    vnm <- paste0(vars, '.', pars)

    don <- lapply(seq_along(aws), function(j){
        dat <- filterAWS_AggrData(tstep, aws[j], start, end, aws_net[j], aws_dir)
        if(is.null(dat)) return(NULL)

        list(date = dat[["date"]], data = dat$data[[vnm]])
    })

    inull <- sapply(don, is.null)
    don0 <- data.frame(Date = NA, Status = "no.data")
    if(!all(inull)){
        don <- don[!inull]
        aws <- aws[!inull]

        daty <- don[[1]]$date
        don <- do.call(cbind, lapply(don, '[[', 'data'))
        ina <- colSums(!is.na(don)) > 0
        if(any(ina)){
            don <- don[, ina, drop = FALSE]
            aws <- aws[ina]
            don0 <- data.frame(daty, don)
            don0[is.na(don0)] <- -99
            names(don0) <- c("Date", aws)
            rownames(don0) <- NULL
        }
    }

    return(convCSV(don0))
}

##########
#' Get aggregated data.
#'
#' Get aggregated data displayed on the chart for download.
#' 
#' @param tstep time step.
#' @param aws AWS ID.
#' @param vars variable.
#' @param start start date.
#' @param end end date.
#' @param aws_net AWS network.
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return CSV format object
#' 
#' @export

downAWSAggrOneVarCSV <- function(tstep, aws, vars, start, end, aws_net, aws_dir){
    don0 <- data.frame(Date = NA, Status = "no.data")
    don <- filterAWS_AggrData(tstep, aws, start, end, aws_net, aws_dir)

    if(!is.null(don)){
        daty <- don[["date"]]

        if(vars == "RR"){
            vnm <- "RR.Tot"
            if(vnm %in% names(don$data)){
                dcsv <- don$data[[vnm]]
                dcsv[is.na(dcsv)] <- -99
                don0 <- data.frame(daty, dcsv)
                names(don0) <- c("Date", vnm)
            }
        }else{
            vnm <- paste0(vars, ".", c("Min", "Max", "Ave"))
            ix <- match(vnm, names(don$data))
            ii <- which(!is.na(ix))
            if(length(ii) > 0){
                dcsv <- data.frame(matrix(NA, ncol = 3, nrow = length(daty)))
                dcsv[, ii] <- don$data[, ix[ii], drop = FALSE]
                dcsv <- round(dcsv, 1)
                dcsv[is.na(dcsv)] <- -99
                don0 <- data.frame(daty, dcsv)
                names(don0) <- c("Date", "Min", "Max", "Ave")
           }
        }
    }

    return(convCSV(don0))
}

##########
#' Get aggregated data in CDT format.
#'
#' Get aggregated data in CDT format for download.
#' 
#' @param tstep time step.
#' @param vars variable.
#' @param pars parameter.
#' @param start start date.
#' @param end end date.
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return CSV object
#' 
#' @export

downAWSAggrCDTDataCSV <- function(tstep, vars, pars, start, end, aws_dir)
{
    dirSTEP <- switch(tstep,
                      "hourly" = "Hourly",
                      "daily" = "Daily",
                      "pentad" = "Pentadal",
                      "dekadal" = "Dekadal",
                      "monthly" = "Monthly"
                    )
    netAWS <- c("REMA", "LSI-ELOG", "LSI-XLOG")
    dirAggrAWS <- file.path(aws_dir, "PROC", "TIMESERIES", dirSTEP, netAWS)

    #######
    coords <- readCoordsAWS(aws_dir)
    coords <- coords[, c(1, 3, 4)]
    coords[, 2:3] <- apply(coords[, 2:3], 2, as.numeric)

    ########
    awsPath <- list.files(dirAggrAWS, ".+\\.rds$", full.names = TRUE, recursive = FALSE)
    awslist <- gsub("\\.rds", "", basename(awsPath))
    icrd <- match(awslist, coords[, 1])
    crd <- as.matrix(coords[icrd, ])
    crd[, 1] <- awslist
    xhead <- t(crd)
    capt <- c("AWS_ID", "LON", "DATE/LAT")

    don0 <- cbind(capt, NA)
    dimnames(don0) <- NULL

    ########
    daty <- format_Seq_Dates(tstep, start, end)

    data.aws <- lapply(awsPath, function(path){
        don <- readRDS(path)
        dat <- don$data[[vars]][[pars]]
        if(is.null(dat)) return(NULL)

        idaty <- match(daty, don[["date"]])
        dat[idaty]
    })

    inull <- sapply(data.aws, is.null)
    if(all(inull)) return(convCSV(don0, FALSE))

    data.aws <- data.aws[!inull]
    xhead <- xhead[, !inull, drop = FALSE]
    data.aws <- do.call(cbind, data.aws)

    ina <- rowSums(!is.na(data.aws)) > 0
    if(!any(ina)) return(convCSV(don0, FALSE))

    data.aws <- data.aws[ina, , drop = FALSE]
    daty <- daty[ina]

    ina <- colSums(!is.na(data.aws)) > 0
    data.aws <- data.aws[, ina, drop = FALSE]
    xhead <- xhead[, ina, drop = FALSE]

    data.aws <- rbind(cbind(capt, xhead), cbind(daty, data.aws))
    data.aws[is.na(data.aws)] <- -99
    dimnames(data.aws) <- NULL

    return(convCSV(data.aws, FALSE))
}

##########
#' Compute precipitation accumulation.
#'
#' Compute precipitation accumulation for download.
#' 
#' @param tstep time basis to accumulate the data.
#' @param aws AWS ID.
#' @param start start date.
#' @param end end date.
#' @param accumul accumulation duration.
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return CSV object
#' 
#' @export

downRainAccumulTS <- function(tstep, aws, start, end, accumul, aws_dir){
    don <- tsRainAccumulAWS(tstep, aws, start, end, accumul, aws_dir)
    if(!is.null(don$data)){
        frmt <- switch(tstep, "hourly" = "%Y%m%d%H", "daily" = "%Y%m%d")
        tt <- format(don$date, frmt)
        don <- data.frame(tt, don$data)
        don[is.na(don)] <- ""
        frmt <- switch(tstep, "hourly" = "Hour", "daily" = "Day")
        names(don) <- c("Date", paste0("Accumulation_", accumul, "-", frmt))
    }else don <- data.frame(status = "no.data")

    return(convCSV(don))
}

##########
#' Get wind data.
#'
#' Get wind data for download.
#' 
#' @param tstep time step of the data.
#' @param aws AWS ID.
#' @param start start time.
#' @param end end time.
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return CSV object
#' 
#' @export

downWindBarbCSV <- function(tstep, aws, start, end, aws_dir){
    don <- getWindData(tstep, aws, start, end, aws_dir)
    wind <- data.frame(status = "no.data")
    if(!is.null(don)){
        wind <- don$wind
        wind$date <- format(wind$date, "%Y%m%d%H%M%S")
    }

    return(convCSV(wind))
}

##########
#' Get wind frequency.
#'
#' Get wind frequency for download.
#' 
#' @param tstep time step of the data.
#' @param aws AWS ID.
#' @param start start time.
#' @param end end time.
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return CSV object
#' 
#' @export

donwWindFreqCSV <- function(tstep, aws, start, end, aws_dir){
    don <- formatFreqTable(tstep, aws, start, end, aws_dir)
    if(is.null(don)){
        ## handle error
        return(NULL)
    }

    nom <- names(don)
    wind <- as.data.frame(don)
    names(wind) <- nom

    return(convCSV(wind))
}

########################### Spatial Data ###########################

#' Get AWS 10 minutes spatial data.
#'
#' Get AWS 10 minutes spatial data for download.
#' 
#' @param time the time to download in the format "YYYY-MM-DD-HH-MM".
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return CSV object
#' 
#' @export

downAWS10MinSpCSV <- function(time, aws_dir){
    don <- spatial10minAWS(time, aws_dir)
    if(don$status == "ok"){
        don <- don$data
        don[is.na(don)] <- ""
    }else data.frame(status = "no.data")

    return(convCSV(don))
}

############
#' Get AWS aggregated spatial data.
#'
#' Get AWS aggregated spatial data for download.
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
#' @return CSV object
#' 
#' @export

downAWSAggrSpCSV <- function(tstep, time, aws_dir){
    don <- spatialAggrAWS(tstep, time, aws_dir)
    if(don$status == "ok"){
        don <- don$data
        don[is.na(don)] <- ""
    }else data.frame(status = "no.data")

    return(convCSV(don))
}

##########
#' Compute precipitation accumulation.
#'
#' Compute precipitation accumulation for download.
#' 
#' @param tstep time basis to accumulate the data.
#' @param time target date.
#' @param accumul accumulation duration.
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return CSV object
#' 
#' @export

downRainAccumulSP <- function(tstep, time, accumul, aws_dir){
    don <- spRainAccumulAWS(tstep, time, accumul, aws_dir)
    if(don$status == "ok"){
        don <- don$data
        don[is.na(don)] <- ""
    }else don <- data.frame(status = "no.data")

    return(convCSV(don))
}

##########
#' Compute hourly mean sea level pressure.
#'
#' Compute hourly mean sea level pressure
#' 
#' @param time target date.
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return CSV object
#' 
#' @export

downMSLPHourly <- function(time, aws_dir){
    don <- compute_mslp(time, aws_dir)

    if(don$status == "ok"){
        don <- don$data
        don[is.na(don)] <- ""
    }else don <- data.frame(status = "no.data")

    return(convCSV(don))
}
