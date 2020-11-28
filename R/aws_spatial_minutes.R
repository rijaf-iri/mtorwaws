
#' Create 10 minutes spatial data.
#'
#' Create 10 minutes spatial data.
#' 
#' @param dirAWS full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @export

aws_spatial_minutes <- function(dirAWS){
    tz <- "Africa/Kigali"
    Sys.setenv(TZ = tz)

    dirOUT <- file.path(dirAWS, "PROC", "SPATIAL", "Minutes")
    logPROC <- file.path(dirAWS, "PROC", "SPATIAL", "processing_Minutes.txt")

    timeNow <- Sys.time()
    daty2 <- paste0(substr(format(timeNow, "%Y%m%d%H%M"), 1, 11), 0)
    daty2 <- strptime(daty2, "%Y%m%d%H%M", tz = tz)
    ## operational last 24 hours
    timeLast <- timeNow - 86400
    daty1 <- paste0(substr(format(timeLast, "%Y%m%d%H%M"), 1, 11), 0)
    daty1 <- strptime(daty1, "%Y%m%d%H%M", tz = tz)

    datys <- seq(daty1, daty2, '10 min')
    datyf <- format(datys, "%Y%m%d%H%M")
    pathMin <- file.path(dirOUT, paste0(datyf, ".rds"))
    ifiles <- file.exists(pathMin)
    if(!any(ifiles)){
        msg <- "No data to update"
        format_out_msg(msg, logPROC)
        return(NULL)
    }

    start_time <- format(datys[rev(which(ifiles))[1]], "%Y-%m-%d %H:%M")
    end_time <- format(timeNow, "%Y-%m-%d %H:%M")

    ret <- try(aws_spatial_10min(start_time, end_time, dirAWS), silent = TRUE)
    if(inherits(ret, "try-error")){ 
        msg <- paste(ret, "Unable to process data from",
                     start_time, "to", end_time)
        format_out_msg(msg, logPROC)
    }
}

#' Create 10 minutes spatial data.
#'
#' Create 10 minutes spatial data archive mode.
#'
#' @param start_time the start time to process in the format "YYYY-MM-DD HH:MM".
#'                  Example: "2019-12-15 12:50"
#' @param end_time  the end time to process in the format "YYYY-MM-DD HH:MM"
#' @param dirAWS full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @export

aws_spatial_minutes_arch <- function(start_time, end_time, dirAWS){
    logPROC <- file.path(dirAWS, "PROC", "SPATIAL", "processing_Minutes.txt")

    ret <- try(aws_spatial_10min(start_time, end_time, dirAWS), silent = TRUE)
    if(inherits(ret, "try-error")){
        msg <- paste(ret, "Unable to process data from",
                     start_time, "to", end_time)
        format_out_msg(msg, logPROC)
    }
}

#####################

aws_spatial_10min <- function(start_time, end_time, dirAWS){
    tz <- "Africa/Kigali"
    time1 <- strptime(start_time, "%Y-%m-%d %H:%M", tz = tz)
    time2 <- strptime(end_time, "%Y-%m-%d %H:%M", tz = tz)
    seqTime <- seq(time1, time2, "10 min")
    seqTime <- format(seqTime, "%Y%m%d%H%M")

    netAWS <- c("REMA", "LSI-ELOG", "LSI-XLOG")

    dirMin <- file.path(dirAWS, "PROC", "TIMESERIES", "Minutes", netAWS)
    dirOUT <- file.path(dirAWS, "PROC", "SPATIAL", "Minutes")
    if(!dir.exists(dirOUT))
        dir.create(dirOUT, showWarnings = FALSE, recursive = TRUE)

    dirTMP <- file.path(dirAWS, "PROC", "SPATIAL", "TMP0")
    if(!dir.exists(dirTMP))
        dir.create(dirTMP, showWarnings = FALSE, recursive = TRUE)

    crds <- readCoordsAWS(dirAWS)

    awsPath <- list.files(dirMin, ".+\\.rds$", full.names = TRUE, recursive = FALSE)
    awsID <- gsub("\\.rds", "", basename(awsPath))
    iaws <- awsID %in% crds$id
    if(!any(iaws)) return(NULL)
    awsPath <- awsPath[iaws]
    awsID <- awsID[iaws]

    #########
    awsPath <- awsPath[110:152]
    awsID <- awsID[110:152]

    parsL <- doparallel.cond(length(awsPath) > 20)
    retLoop <- cdtforeach(seq_along(awsPath), parsL, FUN = function(jj){
        dat <- try(readRDS(awsPath[jj]), silent = TRUE)
        if(inherits(dat, "try-error")) return(NULL)

        daty <- strptime(dat$date, "%Y%m%d%H%M%S", tz = tz)
        idaty <- daty >= time1 & daty <= time2
        if(!any(idaty)) return(NULL)

        index <- index_min2min(dat$date[idaty], 10)

        for(ii in seq_along(index)){
            y <- lapply(dat$data, function(x){
                x <- x[idaty, , drop = FALSE]
                ix <- index[[ii]]
                x <- x[ix, , drop = FALSE]
                if(length(ix) > 1){
                    nx <- names(x)
                    res <- lapply(nx, function(n){
                        foo <- switch(n, 'Tot' = sum, 'Min' = min,
                                      'Max' = max, 'Ave' = mean, mean)
                        y <- suppressWarnings(foo(x[[n]], na.rm = TRUE))
                        y[is.nan(y) | is.infinite(y)] <- NA
                        y
                    })
                    names(res) <- nx

                    x <- do.call(cbind.data.frame, res)
                }

                return(x)
            })

            y <- lapply(y, function(x){
                x <- x[, !is.na(x), drop = FALSE]
                if(ncol(x) == 0) return(NULL)
                as.list(x)
            })

            inull <- sapply(y, is.null)
            if(all(inull)) next
            y <- y[!inull]
            fltmp <- file.path(dirTMP, paste0(names(index[ii]), '_', awsID[jj]))
            saveRDS(y, file = fltmp)
        }
    })

    # parsL <- doparallel.cond(length(seqTime) > 200)
    # retLoop <- cdtforeach(seq_along(seqTime), parsL, FUN = function(tt){
    #     awsList <- file.path(dirTMP, paste0(tt, '_', awsID))
    #     ifiles <- file.exists(awsList)
    #     if(!any(ifiles)) return(NULL)
    #     awsList <- awsList[ifiles]
    #     awsIds <- awsID[ifiles]

    #     awsSP <- lapply(awsList, readRDS)
    #     names(awsSP) <- awsIds
    #     out <- list(date = tt, id = awsIds, data = awsSP)
    #     saveRDS(out, file = file.path(dirOUT, paste0(tt, ".rds")))
    # })

    # unlink(dirTMP, recursive = TRUE)
}
