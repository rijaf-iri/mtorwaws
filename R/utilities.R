
rbindListDF <- function(dat){
    NOM <- unique(do.call(c, lapply(dat, names)))
    don <- list()
    for(ii in NOM){
        vrs <- lapply(dat, '[[', ii)
        nom <- unique(do.call(c, lapply(vrs, names)))

        vrs <- lapply(vrs, function(x){
            ix <- !nom %in% names(x)
            if(any(ix)){
                for(nm in nom[ix]) x[[nm]] <- NA
            }
            return(x)
        })

        don[[ii]] <- do.call(rbind, vrs)
    }

    return(don)
}

convertAWSList2DF <- function(don){
    nl <- length(don)
    if(nl == 1) return(don)

    daty <- lapply(don, "[[", "date")
    daty <- do.call(c, daty)

    dat <- lapply(don, "[[", "data")
    data.aws <- rbindListDF(dat)

    list(date = daty, data = data.aws)
}

combineAWS2DF <- function(data1, data2){
    daty <- c(data1$date, data2$date)
    don <- list(data1$data, data2$data)
    data.aws <- rbindListDF(don)

    idaty <- duplicated(daty, fromLast = TRUE)
    daty <- daty[!idaty]
    data.aws <- lapply(data.aws, function(vr) vr[!idaty, , drop = FALSE])
    ix <- order(daty)
    daty <- daty[ix]
    data.aws <- lapply(data.aws, function(vr) vr[ix, , drop = FALSE])

    list(date = daty, data = data.aws)
}

format_out_msg <- function(msg, logfile, append = TRUE){
    ret <- c(paste("Time:", Sys.time(), "\n"), msg, "\n",
             "*********************************\n")
    cat(ret, file = logfile, append = append)
}

index_min2min <- function(times, out_step){
    ttn <- as.numeric(substr(times, 11, 12))
    ttn <- floor(ttn / out_step) * out_step
    ttn <- stringr::str_pad(ttn, 2, pad = "0")
    index <- split(seq_along(ttn), paste0(substr(times, 1, 10), ttn))

    return(index)
}

split.date.by.day <- function(start_time, end_time, tz = "Africa/Kigali"){
    start_time <- strptime(start_time, "%Y-%m-%d %H:%M", tz = tz)
    daty1 <- paste0(substr(format(start_time, "%Y%m%d%H%M"), 1, 11), 0)
    daty1 <- strptime(daty1, "%Y%m%d%H%M", tz = tz)
    end_time <- strptime(end_time, "%Y-%m-%d %H:%M", tz = tz)
    daty2 <- paste0(substr(format(end_time, "%Y%m%d%H%M"), 1, 11), 0)
    daty2 <- strptime(daty2, "%Y%m%d%H%M", tz = tz)

    datys <- seq(daty1, daty2, '10 min')

    daty_d <- format(datys, "%Y%m%d")
    index <- split(seq_along(daty_d), daty_d)
    daty_s <- lapply(index, function(im){
        x <- datys[im]
        c(format(x[1], "%Y-%m-%d %H:%M"),
          format(x[length(x)], "%Y-%m-%d %H:%M"))
    })
    # daty_s[[1]][1] <- paste(substr(daty_s[[1]][1], 1, 10), format(start_time, "%H:%M"))
    # daty_s[[length(daty_s)]][2] <- paste(substr(daty_s[[length(daty_s)]][2], 1, 10),
    #                                      format(end_time, "%H:%M"))

    return(daty_s)
}

###################

doparallel.cond <- function(condition,
                            parll = list(dopar = TRUE,
                                         detect.cores = FALSE,
                                         nb.cores = 4)
                           )
{
    c(condition = condition, parll)
}

cdtdoparallel <- function(condition, dopar = TRUE,
                          detect.cores = FALSE, nb.cores = 4)
{
    okpar <- FALSE
    if(dopar){
        cores <- parallel::detectCores()
        if(detect.cores){
            nb.cores <- cores - 1
            okpar <- if(nb.cores >= 2) TRUE else FALSE
        }else{
            okpar <- if(cores >= 2 && nb.cores >= 2) TRUE else FALSE
        }
    }

    if(okpar & condition){
        klust <- parallel::makeCluster(nb.cores)
        doParallel::registerDoParallel(klust)
        `%dofun%` <- foreach::`%dopar%`
        closeklust <- TRUE
    }else{
        klust <- NULL
        `%dofun%` <- foreach::`%do%`
        closeklust <- FALSE
    }

    list(dofun = `%dofun%`, cluster = klust, parLL = closeklust)
}

utils::globalVariables(c('jloop'))

cdtforeach <- function(loopL, parsL, ..., FUN){
    FUN <- match.fun(FUN)
    if(missing(parsL)) parsL <- list(condition = FALSE)
    is.parallel <- do.call(cdtdoparallel, parsL)

    if(is.parallel$parLL){
        on.exit(parallel::stopCluster(is.parallel$cluster))
        `%parLoop%` <- is.parallel$dofun
        ret.loop <- foreach::foreach(jloop = loopL, ...) %parLoop% FUN(jloop)
    }else{
        ret.loop <- lapply(loopL, function(jloop){
            ret <- FUN(jloop)
            return(ret)
        })
    }

    return(ret.loop)
}
