
convertAWSList2DF <- function(don){
    nl <- length(don)
    if(nl == 1) return(don)

    daty <- lapply(don, "[[", "date")
    daty <- do.call(c, daty)

    dat <- lapply(don, "[[", "data")
    NOM <- unique(do.call(c, lapply(dat, names)))

    data.aws <- list()
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

        data.aws[[ii]] <- do.call(rbind, vrs)
    }

    list(date = daty, data = data.aws)
}

combineAWS2DF <- function(data1, data2){
    daty <- c(data1$date, data2$date)
    don <- list(data1$data, data2$data)
    NOM <- unique(do.call(c, lapply(don, names)))

    data.aws <- list()
    for(ii in NOM){
        vrs <- lapply(don, '[[', ii)
        nom <- unique(do.call(c, lapply(vrs, names)))

        vrs <- lapply(vrs, function(x){
            ix <- !nom %in% names(x)
            if(any(ix)){
                for(nm in nom[ix]) x[[nm]] <- NA
            }
            return(x)
        })

        data.aws[[ii]] <- do.call(rbind, vrs)
    }

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
