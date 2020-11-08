
## operational

## archive

#########
aws_spatial_minutes <- function(start_time, end_time, dirAWS){
    time1 <- strptime(start_time, "%Y-%m-%d %H:%M", tz = "Africa/Kigali")
    time2 <- strptime(end_time, "%Y-%m-%d %H:%M", tz = "Africa/Kigali")
    seqTime <- seq(time1, time2, "10 min")
    pattern <- substr(format(seqTime, "%Y%m%d%H%M"), 1, 11)
    pattern <- paste0(pattern, ".+\\.rds$")

    netAWS <- c("REMA", "LSI-ELOG", "LSI-XLOG")

    dirQC <- file.path(dirAWS, "PROC", "QCOUT", "QC1")
    dirMin <- file.path(dirAWS, "RAW", netAWS, "DATA")
    dirOUT <- file.path(dirAWS, "PROC", "SPATIAL", "Minutes")
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
                x <- readRDS(file.path(path, fl[i]))
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

            dat <- rbindListDF(dat)
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
