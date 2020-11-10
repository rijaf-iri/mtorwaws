
wind2hourly <- function(wind){
    wu <- -wind$ws * sin(pi * wind$wd / 180)
    wv <- -wind$ws * cos(pi * wind$wd / 180)
    index <- split(seq_along(wind$date), substr(wind$date, 1, 10))
    uvhr <- lapply(index, function(i){
        u <- mean(wu[i], na.rm = TRUE)
        v <- mean(wv[i], na.rm = TRUE)
        if(is.nan(u)) u <- NA
        if(is.nan(v)) v <- NA
        c(u, v)
    })
    uvhr <- do.call(rbind, uvhr)
    ff <- sqrt(uvhr[, 1]^2 + uvhr[, 2]^2)
    dd <- (atan2(uvhr[, 1], uvhr[, 2]) * 180/pi) + ifelse(ff < 1e-14, 0, 180)
    ff <- round(ff, 2)
    dd <- round(dd, 2)
    wsd <- data.frame(date = names(index), ws = ff, wd = dd)
    rownames(wsd) <- NULL
    return(wsd)
}

###########################

getWindData <- function(tstep, aws, start_time, end_time, aws_dir){
    don <- readAWSMinuteData(aws, aws_dir)
    daty <- don$date
    wind <- don$data[c('FF', 'DD')]
    wind <- switch(don$AWSGroup,
                   "REMA" = data.frame(ws = wind$FF$Ave, wd = wind$DD$Ave),
                   "LSI-ELOG" = data.frame(ws = wind$FF$Ave, wd = wind$DD$RisDir),
                   "LSI-XLOG" = data.frame(ws = wind$FF$Ave, wd = wind$DD$Ave))
    wind$wd[wind$ws == 0] <- 0
    dts <- strptime(daty, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
    if(tstep == "hourly"){
        start <- strptime(start_time, "%Y-%m-%d-%H", tz = "Africa/Kigali")
        end <- strptime(end_time, "%Y-%m-%d-%H", tz = "Africa/Kigali")
    }else{
        start <- strptime(start_time, "%Y-%m-%d-%H-%M", tz = "Africa/Kigali")
        end <- strptime(end_time, "%Y-%m-%d-%H-%M", tz = "Africa/Kigali")
    }
    ix <- dts >= start & dts <= end
    wind <- wind[ix, , drop = FALSE]
    if(nrow(wind) == 0) return(NULL)

    wind <- cbind(date = daty[ix], wind)
    dts <- dts[ix]

    ######### 

    if(tstep == "hourly"){
        wind <- wind2hourly(wind)
        dts <- strptime(wind$date, "%Y%m%d%H", tz = "Africa/Kigali")
        daty <- seq(start, end, 'hour')
        tstep.out <- 1
    }else{
        daty <- seq(start, end, paste(don$timestep, 'min'))
        tstep.out <- don$timestep
    }

    ######### 

    ddif <- diff(dts)
    idf <- ddif > tstep.out
    if(any(idf)){
        idt <- which(idf)
        addmul <- if(tstep == "hourly") 3600 else tstep.out * 60
        miss.daty <- dts[idt] + addmul
        miss.daty <- format(miss.daty, "%Y%m%d%H%M%S", tz = "Africa/Kigali")

        daty1 <- rep(NA, length(dts) + length(miss.daty))
        wind1 <- data.frame(date = daty1, ws = daty1, wd = daty1)
        daty1[idt + seq(length(miss.daty))] <- miss.daty
        ix <- is.na(daty1)
        daty1[ix] <- format(dts, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
        wind1[, 1] <- daty1
        wind1[ix, -1] <- wind[, -1]
        wind <- wind1
        dts <- strptime(wind$date, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
    }

    ######### 

    wind$date <- dts
    avail <- round(100 * sum(!is.na(wind$ws)) / length(daty), 1)

    info <- c(don[c('id', 'stationName', 'AWSGroup', 'timestep')], list(avail = avail))
    return(c(info, list(wind = wind)))
}

###########################

windFrequencyTable <- function(tstep, aws, start, end, aws_dir){
    don <- getWindData(tstep, aws, start, end, aws_dir)
    if(is.null(don)){
        ## handle error
        return(NULL)
    }

    winds <- don$wind
    winds <- winds[!is.na(winds$ws) & !is.na(winds$wd), , drop = FALSE]
    nobs <- nrow(winds)
    if(nobs == 0){
        ## handle error
        return(NULL)
    }
    icalm <- winds$ws == 0 | winds$wd == 0
    mean.s <- mean(winds$ws)
    wcalm <- winds[icalm, , drop = FALSE]
    ncalm <- nrow(wcalm)
    freq.calm <- if(ncalm == 0) 0 else 100 * ncalm/nobs

    #####
    winds <- winds[!icalm, , drop = FALSE]
    nobs <- nrow(winds)

    s.breaks <- c(0, 0.5, 2, 4, 6, 8, 12, 100)
    s.labels <- c(" < 0.5", "0.5 to 2", "2 to 4", "4 to 6", "6 to 8", "8 to 12", " > 12")

    dres <- 22.5
    d.breaks <- c(0, seq(dres/2, 360 - dres/2, by = dres), 360)
    d.labels <- c("N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW", "N")

    s.binned <- cut(x = winds$ws, breaks = s.breaks, labels = s.labels, ordered_result = TRUE)
    d.binned <- cut(x = winds$wd, breaks = d.breaks, ordered_result = TRUE)
    levels(d.binned) <- d.labels

    freq <- table(d.binned, s.binned)
    freq <- 100 * freq/nobs

    list(id = don$id, name = don$stationName, timestep = don$timestep,
         calm = freq.calm, mean = mean.s, freq = freq, avail = don$avail)
}
