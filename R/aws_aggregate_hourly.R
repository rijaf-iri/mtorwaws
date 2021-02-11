
#' Aggregate data to hourly.
#'
#' Aggregate AWS data to hourly.
#' 
#' @param dirAWS full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' @param netAWS the name of the AWS network, "LSI-ELOG", "LSI-XLOG" or "REMA".
#' @param archive TRUE for archive mode.
#' 
#' @export

aggregate2hourly <- function(dirAWS, netAWS, archive = FALSE){
    tz <- "Africa/Kigali"
    mfracFile <- file.path(dirAWS, "PARAMS", "Min_Frac_Hourly.json")
    minFrac <- jsonlite::read_json(mfracFile)

    dirMin <- file.path(dirAWS, "PROC", "TIMESERIES", "Minutes", netAWS)
    dirInfo <- file.path(dirAWS, "RAW", netAWS, "INFO")
    dirHour <- file.path(dirAWS, "PROC", "TIMESERIES", "Hourly", netAWS)
    if(!dir.exists(dirHour))
        dir.create(dirHour, showWarnings = FALSE, recursive = TRUE)

    awsList <- list.files(dirMin, ".+\\.rds$")

    for(aws in awsList){
        info <- readRDS(file.path(dirInfo, aws))
        tstep <- ifelse(is.na(info$tstep) | info$tstep < 5, 10, info$tstep)
        nbstep <- 60 / tstep
     
        file_mn <- file.path(dirMin, aws)
        dat_mn <- try(readRDS(file_mn), silent = TRUE)
        if(inherits(dat_mn, "try-error")) next
        daty <- strptime(dat_mn$date, "%Y%m%d%H%M%S", tz = tz)

        file_hr <- file.path(dirHour, aws)

        if(file.exists(file_hr)){
            dat_hr <- try(readRDS(file_hr), silent = TRUE)
            if(inherits(dat_hr, "try-error")) next
            nhr <- length(dat_hr$date)

            if(archive){
                ## archive
                fdaty <- format(daty, "%Y%m%d%H")
                idaty <- !fdaty %in%  dat_hr$date
                ihr <- dat_hr$date %in% fdaty
                dat_hr$date <- dat_hr$date[ihr]
                dat_hr$data <- lapply(dat_hr$data, function(x) x[ihr, , drop = FALSE])
            }else{
                ## operational
                timeLast <- dat_hr$date[nhr]
                timeLast <- strptime(timeLast, "%Y%m%d%H", tz = tz)
                dat_hr$date <- dat_hr$date[-nhr]
                dat_hr$data <- lapply(dat_hr$data, function(x) x[-nhr, , drop = FALSE])
                idaty <- daty >= timeLast
            }
        }else{
            dat_hr <- NULL
            idaty <- rep(TRUE, length(daty))
        }

        daty <- daty[idaty]
        if(length(daty) == 0) next

        vars0 <- c('RR', 'TT', 'RH', 'RAD', 'PRES', 'FF')
        don <- dat_mn$data[names(dat_mn$data) %in% vars0]
        pars0 <- c('Tot', 'Min', 'Max', 'Ave')
        don <- lapply(don, function(x){
            ix <- names(x) %in% pars0
            x[idaty, ix, drop = FALSE]
        })

        times <- format(daty, "%Y%m%d%H")
        index <- split(seq_along(times), times)
        odaty <- names(index)
        NOM <- names(don)

        aggr <- lapply(index, function(ix){
            frac <- length(ix) / nbstep
            ret <- lapply(NOM, function(n){
                x <- don[[n]]
                if(n == "RR"){
                    out <- data.frame(Tot = NA * 0.)
                    if(frac >= minFrac[[n]]){
                        x <- x$Tot[ix]
                        x <- x[!is.na(x)]
                        if(length(x) / nbstep >= minFrac[[n]])
                            out$Tot <- sum(x)
                    }
                }else{
                    out <- data.frame(matrix(NA * 0., ncol = 3))
                    nom <- c('Ave', 'Max', 'Min')
                    names(out) <- nom
                    if(frac >= minFrac[[n]]){
                        nc <- ncol(x)
                        if(nc == 1){
                            x <- x[ix, 1]
                            x <- x[!is.na(x)]
                            if(length(x) / nbstep >= minFrac[[n]]){
                                out$Ave <- mean(x)
                                out$Max <- max(x)
                                out$Min <- min(x)
                            }
                        }else{
                            x <- x[ix, , drop = FALSE]
                            if(nc == 2){
                                if(all(c('Ave', 'Min') %in% names(x))){
                                    x$Max <- x$Ave
                                }
                                if(all(c('Ave', 'Max') %in% names(x))){
                                    x$Min <- x$Ave
                                }
                                if(all(c('Min', 'Max') %in% names(x))){
                                    x$Ave <- x$Max
                                }
                            }

                            nna <- colSums(!is.na(x)) / nbstep >= minFrac[[n]]
                            for(p in nom){
                                if(nna[[p]]){
                                    fun <- switch(p, 'Ave' = mean, 'Max' = max, 'Min' = min)
                                    out[[p]] <- fun(x[[p]], na.rm = TRUE)
                                }
                            }
                        }
                    }
                }

                return(out)
            })
            names(ret) <- NOM

            return(ret)
        })

        aggr <- lapply(NOM, function(n){
            x <- lapply(aggr, '[[', n)
            x <- do.call(rbind, x)
            x <- round(x, 1)
            rownames(x) <- NULL
            x
        })
        names(aggr) <- NOM
        dat_new <- list(date = odaty, data = aggr)
        if(!is.null(dat_hr))
            dat_new <- combineAWS2DF(dat_hr, dat_new)

        con <- gzfile(file_hr, compression = 9)
        open(con, "wb")
        saveRDS(dat_new, con)
        close(con)
    }
}
