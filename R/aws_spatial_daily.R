
#' Create daily spatial data.
#'
#' Create daily spatial data.
#' 
#' @param dirAWS full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @export

aws_spatial_daily <- function(dirAWS){
    dirOUT <- file.path(dirAWS, "PROC", "SPATIAL", "Daily")
    logPROC <- file.path(dirAWS, "PROC", "SPATIAL", "processing_Daily.txt")

    timeNow <- Sys.time()
    daty2 <- as.Date(format(timeNow, "%Y%m%d"), "%Y%m%d")
    ## operational last 5 days
    daty1 <- daty2 - 5

    datys <- seq(daty1, daty2, 'day')
    datyf <- format(datys, "%Y%m%d")
    pathDay <- file.path(dirOUT, paste0(datyf, ".rds"))
    ifiles <- file.exists(pathDay)
    if(!any(ifiles)){
        msg <- "No data to update"
        format_out_msg(msg, logPROC)
        return(NULL)
    }

    start_date <- format(datys[rev(which(ifiles))[1]], "%Y-%m-%d")
    end_date <- format(daty2, "%Y-%m-%d")

    ret <- try(aws_spatial_day(start_date, end_date, dirAWS), silent = TRUE)
    if(inherits(ret, "try-error")){ 
        msg <- paste(ret, "Unable to process data from",
                     start_date, "to", end_date)
        format_out_msg(msg, logPROC)
    }
}

#' Create daily spatial data.
#'
#' Create daily spatial data archive mode.
#'
#' @param start_date the start date to process in the format "YYYY-MM-DD".\cr
#'                  Example: "2019-12-15"
#' @param end_date  the end date to process in the format "YYYY-MM-DD"
#' @param dirAWS full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @export

aws_spatial_daily_arch <- function(start_date, end_date, dirAWS){
    logPROC <- file.path(dirAWS, "PROC", "SPATIAL", "processing_Daily.txt")
    ret <- try(aws_spatial_day(start_date, end_date, dirAWS), silent = TRUE)
    if(inherits(ret, "try-error")){
        msg <- paste(ret, "Unable to process data from",
                     start_date, "to", end_date)
        format_out_msg(msg, logPROC)
    }
}

aws_spatial_day <- function(start_date, end_date, dirAWS){
    time1 <- as.Date(start_date, "%Y-%m-%d")
    time2 <- as.Date(end_date, "%Y-%m-%d")
    seqTime <- seq(time1, time2, "day")
    seqTime <- format(seqTime, "%Y%m%d")

    netAWS <- c("REMA", "LSI-ELOG", "LSI-XLOG")

    dirDay <- file.path(dirAWS, "PROC", "TIMESERIES", "Daily", netAWS)
    dirOUT <- file.path(dirAWS, "PROC", "SPATIAL", "Daily")
    if(!dir.exists(dirOUT))
        dir.create(dirOUT, showWarnings = FALSE, recursive = TRUE)

    dirTMP <- file.path(dirAWS, "PROC", "SPATIAL", "TMP2")
    if(!dir.exists(dirTMP))
        dir.create(dirTMP, showWarnings = FALSE, recursive = TRUE)

    crds <- readCoordsAWS(dirAWS)

    awsPath <- list.files(dirDay, ".+\\.rds$", full.names = TRUE, recursive = FALSE)
    awsID <- gsub("\\.rds", "", basename(awsPath))
    iaws <- awsID %in% crds$id
    if(!any(iaws)) return(NULL)
    awsPath <- awsPath[iaws]
    awsID <- awsID[iaws]

    for(jj in seq_along(awsPath)){
        dat <- try(readRDS(awsPath[jj]), silent = TRUE)
        if(inherits(dat, "try-error")) next

        daty <- as.Date(dat$date, "%Y%m%d")
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
