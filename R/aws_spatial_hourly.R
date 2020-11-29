
#' Create hourly spatial data.
#'
#' Create hourly spatial data.
#' 
#' @param dirAWS full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @export

aws_spatial_hourly <- function(dirAWS){
    tz <- "Africa/Kigali"
    Sys.setenv(TZ = tz)

    dirOUT <- file.path(dirAWS, "PROC", "SPATIAL", "Hourly")
    logPROC <- file.path(dirAWS, "PROC", "SPATIAL", "processing_Hourly.txt")

    timeNow <- Sys.time()
    daty2 <- strptime(format(timeNow, "%Y%m%d%H"), "%Y%m%d%H", tz = tz)
    ## operational last 6 hours
    timeLast <- timeNow - 21600
    daty1 <- strptime(format(timeLast, "%Y%m%d%H"), "%Y%m%d%H", tz = tz)

    datys <- seq(daty1, daty2, 'hour')
    datyf <- format(datys, "%Y%m%d%H")
    pathHr <- file.path(dirOUT, paste0(datyf, ".rds"))
    ifiles <- file.exists(pathHr)
    if(!any(ifiles)){
        msg <- "No data to update"
        format_out_msg(msg, logPROC)
        return(NULL)
    }

    start_time <- format(datys[rev(which(ifiles))[1]], "%Y-%m-%d %H:00")
    end_time <- format(timeNow, "%Y-%m-%d %H:00")

    ret <- try(aws_spatial_hour(start_time, end_time, dirAWS), silent = TRUE)
    if(inherits(ret, "try-error")){ 
        msg <- paste(ret, "Unable to process data from",
                     start_time, "to", end_time)
        format_out_msg(msg, logPROC)
    }
}

#' Create hourly spatial data.
#'
#' Create hourly spatial data archive mode.
#'
#' @param start_time the start time to process in the format "YYYY-MM-DD HH:00".
#'                  Example: "2019-12-15 12:00"
#' @param end_time  the end time to process in the format "YYYY-MM-DD HH:00"
#' @param dirAWS full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @export

aws_spatial_hourly_arch <- function(start_time, end_time, dirAWS){
    logPROC <- file.path(dirAWS, "PROC", "SPATIAL", "processing_Hourly.txt")
    ret <- try(aws_spatial_hour(start_time, end_time, dirAWS), silent = TRUE)
    if(inherits(ret, "try-error")){
        msg <- paste(ret, "Unable to process data from",
                     start_time, "to", end_time)
        format_out_msg(msg, logPROC)
    }
}

aws_spatial_hour <- function(start_time, end_time, dirAWS){
    tz <- "Africa/Kigali"
    time1 <- strptime(start_time, "%Y-%m-%d %H:%M", tz = tz)
    time2 <- strptime(end_time, "%Y-%m-%d %H:%M", tz = tz)
    seqTime <- seq(time1, time2, "hour")
    seqTime <- format(seqTime, "%Y%m%d%H")

    netAWS <- c("REMA", "LSI-ELOG", "LSI-XLOG")

    dirHr <- file.path(dirAWS, "PROC", "TIMESERIES", "Hourly", netAWS)
    dirOUT <- file.path(dirAWS, "PROC", "SPATIAL", "Hourly")
    if(!dir.exists(dirOUT))
        dir.create(dirOUT, showWarnings = FALSE, recursive = TRUE)

    dirTMP <- file.path(dirAWS, "PROC", "SPATIAL", "TMP1")
    if(!dir.exists(dirTMP))
        dir.create(dirTMP, showWarnings = FALSE, recursive = TRUE)

    crds <- readCoordsAWS(dirAWS)

    awsPath <- list.files(dirHr, ".+\\.rds$", full.names = TRUE, recursive = FALSE)
    awsID <- gsub("\\.rds", "", basename(awsPath))
    iaws <- awsID %in% crds$id
    if(!any(iaws)) return(NULL)
    awsPath <- awsPath[iaws]
    awsID <- awsID[iaws]

    for(jj in seq_along(awsPath)){
        dat <- try(readRDS(awsPath[jj]), silent = TRUE)
        if(inherits(dat, "try-error")) next

        daty <- strptime(dat$date, "%Y%m%d%H", tz = tz)
        index <- which(daty >= time1 & daty <= time2)
        if(length(index) == 0) next

        for(ix in index){
            y <- lapply(dat$data, function(x) x[ix, , drop = FALSE])
            y <- lapply(y, function(x){
                x <- x[, !is.na(x), drop = FALSE]
                if(ncol(x) == 0) return(NULL)
                as.list(x)
            })

            inull <- sapply(y, is.null)
            if(all(inull)) next
            y <- y[!inull]
            fltmp <- file.path(dirTMP, paste0(dat$date[ix], '_', awsID[jj]))
            saveRDS(y, file = fltmp)
        }
    }

    for(tt in seqTime){
        awsList <- file.path(dirTMP, paste0(tt, '_', awsID))
        ifiles <- file.exists(awsList)
        if(!any(ifiles)) next
        awsList <- awsList[ifiles]
        awsIds <- awsID[ifiles]

        awsSP <- lapply(awsList, readRDS)
        names(awsSP) <- awsIds
        out <- list(date = tt, id = awsIds, data = awsSP)
        saveRDS(out, file = file.path(dirOUT, paste0(tt, ".rds")))
    }

    unlink(dirTMP, recursive = TRUE)
}
