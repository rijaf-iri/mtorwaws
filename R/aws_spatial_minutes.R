
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
    ## operational last 6 hours
    # timeLast <- timeNow - 21600
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
                     start_time, "to",end_time)
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
    tz <- "Africa/Kigali"
    daty_s <- split.date.by.day(start_time, end_time, tz)
    logPROC <- file.path(dirAWS, "PROC", "SPATIAL", "processing_Minutes.txt")

    parsL <- doparallel.cond(length(daty_s) > 4)
    retLoop <- cdtforeach(seq_along(daty_s), parsL, FUN = function(jj){
        ret <- try(aws_spatial_10min(daty_s[[jj]][1],
                   daty_s[[jj]][2], dirAWS), silent = TRUE)
        if(inherits(ret, "try-error")){
            msg <- paste(ret, "Unable to process data from",
                         daty_s[[jj]][1], "to", daty_s[[jj]][2])
            format_out_msg(msg, logPROC)
        }
    })
}

aws_spatial_10min <- function(start_time, end_time, dirAWS){
    tz <- "Africa/Kigali"
    time1 <- strptime(start_time, "%Y-%m-%d %H:%M", tz = tz)
    time2 <- strptime(end_time, "%Y-%m-%d %H:%M", tz = tz)
    seqTime <- seq(time1, time2, "10 min")
    pattern <- substr(format(seqTime, "%Y%m%d%H%M"), 1, 11)
    pattern <- paste0(pattern, ".+\\.rds$")

    netAWS <- c("REMA", "LSI-ELOG", "LSI-XLOG")

    dirQC <- file.path(dirAWS, "PROC", "QCOUT", "QC1")
    dirMin <- file.path(dirAWS, "RAW", netAWS, "DATA")
    dirOUT <- file.path(dirAWS, "PROC", "SPATIAL", "Minutes")
    if(!dir.exists(dirOUT))
        dir.create(dirOUT, showWarnings = FALSE, recursive = TRUE)

    crds <- readCoordsAWS(dirAWS)

    awsPath <- list.dirs(dirMin, full.names = TRUE, recursive = FALSE)
    iaws <- basename(awsPath) %in% crds$id
    awsPath <- awsPath[iaws]

    awsIndex <- lapply(awsPath, function(path){
        fileList <- lapply(pattern, function(p) list.files(path, p))
        fileList <- do.call(c, fileList)
        if(length(fileList) == 0)
            return(NULL)
        awsTime <- substr(fileList, 1, 14)
        idx <- index_min2min(awsTime, 10)

        lapply(idx, function(i) fileList[i])
    })

    inull <- sapply(awsIndex, is.null)
    if(all(inull)) return(NULL)

    awsIndex <- awsIndex[!inull]
    awsPath <- awsPath[!inull]
    awsList <- basename(awsPath)

    daty <- lapply(awsIndex, names)
    daty <- unique(do.call(c, daty))

    for(tt in daty){
        timeList <- lapply(awsIndex, '[[', tt)

        inull <- sapply(timeList, is.null)
        if(all(inull)) next

        timeList <- timeList[!inull]
        awsPath <- awsPath[!inull]
        awsList <- awsList[!inull]

        awsSP <- lapply(seq_along(timeList), function(j){
            fl <- timeList[[j]]
            path <- awsPath[j]
            aws <- awsList[j]
            net <- basename(dirname(dirname(path)))
            pqc <- file.path(dirQC, net, aws)

            dat <- lapply(seq_along(fl), function(i){
                fl_dt <- file.path(path, fl[i])
                if(!file.exists(fl_dt)) return(NULL)
                x <- try(readRDS(fl_dt), silent = TRUE)
                if(inherits(x, "try-error")) return(NULL)

                x <- x$data
                fqc <- file.path(pqc, fl[i])

                if(file.exists(fqc)){
                    qc <- readRDS(fqc)
                    qc <- qc$qc
                    for(n in names(qc))
                        x[[n]][names(qc[[n]])] <- NA
                }

                return(x)
            })

            inull <- sapply(dat, is.null)
            if(all(inull)) return(NULL)
            dat <- rbindListDF(dat[!inull])

            if(nrow(dat[[1]]) > 1){
                dat <- lapply(dat, function(x){
                    nx <- names(x)
                    res <- lapply(nx, function(n){
                        foo <- switch(n, 'Tot' = sum, 'Min' = min,
                                      'Max' = max, 'Ave' = mean, mean)
                        y <- suppressWarnings(foo(x[[n]], na.rm = TRUE))
                        y[is.nan(y) | is.infinite(y)] <- NA
                        y
                    })
                    names(res) <- nx

                    do.call(cbind.data.frame, res)
                })
            }

            if(net == "REMA"){
                wnd <- c('FF', 'FFmax')
                if(all(wnd %in% names(dat))){
                    spd <- dat[wnd]
                    dat <- dat[!names(dat) %in% wnd]
                    dat$FF <- cbind(spd$FF, spd$FFmax)
                }

                if('FFmax' %in% names(dat)){
                    nom <- names(dat)
                    nom[nom == "FFmax"] <- "FF"
                    names(dat) <- nom
                }
            }

            dat <- lapply(dat, function(x){
                x <- x[, !is.na(x), drop = FALSE]
                if(ncol(x) == 0) return(NULL)
                as.list(x)
            })

            inull <- sapply(dat, is.null)
            if(all(inull)) return(NULL)

            return(dat[!inull])
        })

        inull <- sapply(awsSP, is.null)
        if(all(inull)) next

        awsSP <- awsSP[!inull]
        awsList <- awsList[!inull]

        names(awsSP) <- awsList
        out <- list(date = tt, id = awsList, data = awsSP)

        saveRDS(out, file = file.path(dirOUT, paste0(tt, ".rds")))
    }
}
