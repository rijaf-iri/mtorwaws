
#' Perform limit check.
#'
#' Perform quality control using limit check method at AWS time step.
#' 
#' @param dirAWS full path to the directory of the parsed data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' @param netAWS the name of the AWS network, "LSI-ELOG", "LSI-XLOG" or "REMA".
#' 
#' @export

qc_limit_check <- function(dirAWS, netAWS){
    tz <- "Africa/Kigali"
    Sys.setenv(TZ = tz)

    limitFile <- file.path(dirAWS, "PARAMS", "QC_Limit_Check.json")
    qcLimPars <- jsonlite::read_json(limitFile)

    dirDATBE <- file.path(dirAWS, "RAW", netAWS, "DATA")
    dirQCBE <- file.path(dirAWS, "PROC", "QCOUT", "QC1")
    dirDATTS <- file.path(dirAWS, "PROC", "TIMESERIES", "Minutes", netAWS)
    dirDATTS_fail <- file.path(dirAWS, "PROC", "TIMESERIES", "Minutes", "AWS-ERROR", netAWS)

    if(!dir.exists(dirDATTS))
        dir.create(dirDATTS, showWarnings = FALSE, recursive = TRUE)
    if(!dir.exists(dirDATTS_fail))
        dir.create(dirDATTS_fail, showWarnings = FALSE, recursive = TRUE)
    logQC <- file.path(dirQCBE, paste0(netAWS, "_LOG.txt"))

    timeNow <- Sys.time()
    awsList <- list.dirs(dirDATBE, full.names = FALSE, recursive = FALSE)

    for(aws in awsList){
        dirDAT <- file.path(dirDATBE, aws)
        dirQC <- file.path(dirQCBE, netAWS, aws)
        if(!dir.exists(dirQC))
            dir.create(dirQC, showWarnings = FALSE, recursive = TRUE)

        fileTS <- file.path(dirDATTS, paste0(aws, ".rds"))
        if(file.exists(fileTS)){
            dataTS <- try(readRDS(fileTS), silent = TRUE)
            if(inherits(dataTS, "try-error")){ 
                heure <- format(timeNow, "%Y%m%d%H")
                fileTS_fail <- file.path(dirDATTS_fail, paste0(aws, "_", heure, ".rds"))
                file.copy(fileTS, fileTS_fail, overwrite = TRUE)
                unlink(fileTS)

                msg <- paste(dataTS, "QC limit Check failed for", aws)
                format_out_msg(msg, logQC)
                next
            }

            timeLast <- dataTS$date[length(dataTS$date)]
            timeLast <- strptime(timeLast, "%Y%m%d%H%M%S", tz = tz)
        }else{
            dataTS <- NULL
            timeLast <- strptime("202010210000", "%Y%m%d%H%M", tz = tz)
        }

        seqTime <- seq(timeLast, timeNow, "min")
        pattern <- format(seqTime, "%Y%m%d%H%M%S.rds")
        pattern_aws <- file.path(dirDAT, pattern)
        ifiles <- file.exists(pattern_aws)
        if(!any(ifiles)) next
        fileList <- pattern[ifiles]

        # seqTime <- seq(timeLast, timeNow, "10 min")
        # pattern <- substr(format(seqTime, "%Y%m%d%H%M"), 1, 11)
        # pattern <- paste0(pattern, ".+\\.rds$")
        # fileList <- lapply(pattern, function(p) list.files(dirDAT, p))
        # fileList <- do.call(c, fileList)
        # if(length(fileList) == 0) next

        # seqTime <- seq(timeLast, timeNow, "10 min")
        # pattern <- substr(format(seqTime, "%Y%m%d%H%M"), 1, 11)
        # pattern <- unique(pattern)
        # pattern <- paste0(pattern, "*")
        # pattern_aws <- file.path(dirDAT, pattern)
        # pattern_aws <- paste('ls -f', pattern_aws, '2>/dev/null')
        # fileList <- suppressWarnings(lapply(pattern_aws, system, intern = TRUE))
        # fileList <- do.call(c, fileList)
        # if(length(fileList) == 0) next
        # fileList <- basename(fileList)

        qcdata <- try(awsQCLimitCheck(fileList, dirDAT, dirQC, qcLimPars), silent = TRUE)
        if(inherits(qcdata, "try-error")){ 
            msg <- paste(qcdata, "QC limit Check failed for", aws)
            format_out_msg(msg, logQC)
            next
        }
        qcdata <- convertAWSList2DF(qcdata)

        if(netAWS == "REMA"){
            don <- qcdata$data
            wnd <- c('FF', 'FFmax')
            if(all(wnd %in% names(don))){
                spd <- don[wnd]
                don <- don[!names(don) %in% wnd]
                don$FF <- cbind(spd$FF, spd$FFmax)
            }

            if('FFmax' %in% names(don)){
                nom <- names(don)
                nom[nom == "FFmax"] <- "FF"
                names(don) <- nom
            }
            qcdata$data <- don
        }

        if(!is.null(dataTS))
            qcdata <- combineAWS2DF(dataTS, qcdata)

        con <- gzfile(fileTS, compression = 9)
        open(con, "wb")
        saveRDS(qcdata, con)
        close(con)
    }
}

#' Perform limit check archive mode.
#'
#' Perform quality control using limit check method at AWS time step.
#'
#' @param start_time the start time to process in the format "YYYY-MM-DD HH:MM".
#'                  Example: "2019-12-15 12:50"
#' @param end_time  the end time to process in the format "YYYY-MM-DD HH:MM"
#' @param dirAWS full path to the directory of the parsed data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' @param netAWS the name of the AWS network, "LSI-ELOG", "LSI-XLOG" or "REMA".
#' 
#' @export

qc_limit_check_arch <- function(start_time, end_time, dirAWS, netAWS){
    tz <- "Africa/Kigali"

    limitFile <- file.path(dirAWS, "PARAMS", "QC_Limit_Check.json")
    qcLimPars <- jsonlite::read_json(limitFile)

    dirDATBE <- file.path(dirAWS, "RAW", netAWS, "DATA")
    dirQCBE <- file.path(dirAWS, "PROC", "QCOUT", "QC1")
    dirDATTS <- file.path(dirAWS, "PROC", "TIMESERIES", "Minutes", netAWS)
    dirDATTS_fail <- file.path(dirAWS, "PROC", "TIMESERIES", "Minutes", "AWS-ERROR", netAWS)

    if(!dir.exists(dirDATTS))
        dir.create(dirDATTS, showWarnings = FALSE, recursive = TRUE)
    if(!dir.exists(dirDATTS_fail))
        dir.create(dirDATTS_fail, showWarnings = FALSE, recursive = TRUE)
    logQC <- file.path(dirQCBE, paste0(netAWS, "_LOG.txt"))

    time1 <- strptime(start_time, "%Y-%m-%d %H:%M", tz = tz)
    time2 <- strptime(end_time, "%Y-%m-%d %H:%M", tz = tz)

    # steps <- switch(netAWS,
    #                 "REMA" = "5 min",
    #                 "LSI-XLOG" = "5 min",
    #                 "LSI-ELOG" = "min")
    # seqTime <- seq(time1, time2, steps)
    seqTime <- seq(time1, time2, "min")
    pattern <- format(seqTime, "%Y%m%d%H%M%S.rds")

    awsList <- list.dirs(dirDATBE, full.names = FALSE, recursive = FALSE)

    for(aws in awsList){
        dirDAT <- file.path(dirDATBE, aws)
        dirQC <- file.path(dirQCBE, netAWS, aws)
        if(!dir.exists(dirQC))
            dir.create(dirQC, showWarnings = FALSE, recursive = TRUE)

        fileTS <- file.path(dirDATTS, paste0(aws, ".rds"))
        oldDATA <- FALSE
        if(file.exists(fileTS)){
            dataTS <- try(readRDS(fileTS), silent = TRUE)
            if(inherits(dataTS, "try-error")){ 
                heure <- format(time1, "%Y%m%d%H")
                fileTS_fail <- file.path(dirDATTS_fail, paste0(aws, "_", heure, ".rds"))
                file.copy(fileTS, fileTS_fail, overwrite = TRUE)
                unlink(fileTS)

                msg <- paste(dataTS, "QC limit Check failed for", aws)
                format_out_msg(msg, logQC)
                next
            }

            oldDATA <- TRUE
        }

        pattern_aws <- file.path(dirDAT, pattern)
        ifiles <- file.exists(pattern_aws)
        if(!any(ifiles)) next
        fileList <- pattern[ifiles]

        qcdata <- try(awsQCLimitCheck(fileList, dirDAT, dirQC, qcLimPars), silent = TRUE)
        if(inherits(qcdata, "try-error")){ 
            msg <- paste(qcdata, "QC limit Check failed for", aws)
            format_out_msg(msg, logQC)
            next
        }
        qcdata <- convertAWSList2DF(qcdata)

        if(netAWS == "REMA"){
            don <- qcdata$data
            wnd <- c('FF', 'FFmax')
            if(all(wnd %in% names(don))){
                spd <- don[wnd]
                don <- don[!names(don) %in% wnd]
                don$FF <- cbind(spd$FF, spd$FFmax)
            }

            if('FFmax' %in% names(don)){
                nom <- names(don)
                nom[nom == "FFmax"] <- "FF"
                names(don) <- nom
            }
            qcdata$data <- don
        }

        if(oldDATA)
            qcdata <- combineAWS2DF(dataTS, qcdata)

        con <- gzfile(fileTS, compression = 9)
        open(con, "wb")
        saveRDS(qcdata, con)
        close(con)
    }
}

awsQCLimitCheck <- function(fileList, dirDAT, dirQC, qcLimPars){
    retqc <- lapply(fileList, function(f){
        x <- readRDS(file.path(dirDAT, f))
        nom <- names(x$data)

        qcout <- lapply(seq_along(x$data), function(j){
            y <- x$data[[j]]
            lim <- qcLimPars$limits[[nom[j]]]
            if(is.null(lim))
                return(list(data = y, qc = NULL))

            q <- y
            q[] <- NA
            for(p in names(lim)){
                if(is.null(y[[p]])) next
                
                if("ValidDataPerc" %in% names(y)){
                    pr <- y[["ValidDataPerc"]]
                    out <- is.na(pr) | pr < qcLimPars$ValidDataPercMin
                    if(out){
                        q[[p]] <- y[[p]]
                        y[[p]] <- NA
                        next
                    }
                }

                out <- !is.na(y[[p]]) & (y[[p]] < lim[[p]]$low | y[[p]] > lim[[p]]$up)
                if(out){
                    q[[p]] <- y[[p]]
                    y[[p]] <- NA
                }
            }

            q <- q[, !is.na(q), drop = FALSE]
            if(ncol(q) == 0) q <- NULL
            list(data = y, qc = q)
        })

        qc <- lapply(qcout, "[[", "qc")
        names(qc) <- nom
        inull <- sapply(qc, is.null)
        if(any(!inull)){
            qc <- qc[!inull]
            qcO <- list(date = x$date, qc = qc)
            saveRDS(qcO, file = file.path(dirQC, f))
        }

        data <- lapply(qcout, "[[", "data")
        names(data) <- nom

        return(list(date = x$date, data = data))
    })

    return(retqc)
}
