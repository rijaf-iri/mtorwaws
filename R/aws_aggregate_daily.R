
#' Aggregate data to daily.
#'
#' Aggregate AWS data to daily.
#' 
#' @param dirAWS full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' @param netAWS the name of the AWS network, "LSI-ELOG", "LSI-XLOG" or "REMA".
#' @param obs_hour daily precipitation observation hour.
#' @param archive TRUE for archive mode.
#' 
#' @export

aggregate2daily <- function(dirAWS, netAWS, obs_hour = 8, archive = FALSE){
    tz <- "Africa/Kigali"
    mfracFile <- file.path(dirAWS, "PARAMS", "Min_Frac_Daily.json")
    minFrac <- jsonlite::read_json(mfracFile)

    dirHour <- file.path(dirAWS, "PROC", "TIMESERIES", "Hourly", netAWS)
    dirDay <- file.path(dirAWS, "PROC", "TIMESERIES", "Daily", netAWS)
    if(!dir.exists(dirDay))
        dir.create(dirDay, showWarnings = FALSE, recursive = TRUE)

    awsList <- list.files(dirHour, ".+\\.rds$")

    for(aws in awsList){
        file_hr <- file.path(dirHour, aws)
        dat_hr <- readRDS(file_hr)
        daty <- strptime(dat_hr$date, "%Y%m%d%H", tz = tz)

        file_day <- file.path(dirDay, aws)

        if(file.exists(file_day)){
            dat_day <- readRDS(file_day)
            nday <- length(dat_day$date)

            if(archive){
                ## archive
                fdaty <- format(daty, "%Y%m%d")
                idaty <- !fdaty %in%  dat_day$date
                iday <- dat_day$date %in% fdaty
                dat_day$date <- dat_day$date[iday]
                dat_day$data <- lapply(dat_day$data, function(x) x[iday, , drop = FALSE])
            }else{
                ## operational
                timeLast <- dat_day$date[nday]
                timeLast <- strptime(timeLast, "%Y%m%d", tz = tz)
                dat_day$date <- dat_day$date[-nday]
                dat_day$data <- lapply(dat_day$data, function(x) x[-nday, , drop = FALSE])
                idaty <- daty >= timeLast
            }
        }else{
            dat_day <- NULL
            idaty <- rep(TRUE, length(daty))
        }

        daty <- daty[idaty]
        if(length(daty) == 0) next

        don <- lapply(dat_hr$data, function(x) x[idaty, , drop = FALSE])

        timesV <- format(daty, "%Y%m%d")
        indexV <- split(seq_along(timesV), timesV)

        timesR <- daty - 3600 * obs_hour
        timesR <- format(timesR, "%Y%m%d%H")
        indexR <- split(seq_along(timesR), substr(timesR, 1, 8))

        odaty <- intersect(names(indexV), names(indexR))
        index <- lapply(odaty, function(tt){
            list(R = indexR[[tt]], V = indexV[[tt]])
        })
        NOM <- names(don)

        aggr <- lapply(index, function(ix){
            ret <- lapply(NOM, function(n){
                x <- don[[n]]
                if(n == "RR"){
                    out <- data.frame(Tot = NA * 0.)
                    if(length(ix$R) / 24 >= minFrac[[n]]){
                        x <- x$Tot[ix$R]
                        x <- x[!is.na(x)]
                        if(length(x) / 24 >= minFrac[[n]])
                            out$Tot <- sum(x)
                    }
                }else{
                    out <- data.frame(matrix(NA * 0., ncol = 3))
                    nom <- c('Ave', 'Max', 'Min')
                    names(out) <- nom
                    if(length(ix$V) / 24 >= minFrac[[n]]){
                        x <- x[ix$V, , drop = FALSE]

                        nna <- colSums(!is.na(x)) / 24 >= minFrac[[n]]
                        for(p in nom){
                            if(nna[[p]]){
                                fun <- switch(p, 'Ave' = mean, 'Max' = max, 'Min' = min)
                                out[[p]] <- fun(x[[p]], na.rm = TRUE)
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

        if(!is.null(dat_day))
            dat_new <- combineAWS2DF(dat_day, dat_new)

        con <- gzfile(file_day, compression = 9)
        open(con, "wb")
        saveRDS(dat_new, con)
        close(con)
    }
}
