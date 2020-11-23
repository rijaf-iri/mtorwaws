
convJSON <- function(obj, ...){
    args <- list(...)
    if(!'pretty' %in% names(args)) args$pretty <- TRUE
    if(!'auto_unbox' %in% names(args)) args$auto_unbox <- TRUE
    if(!'na' %in% names(args)) args$na <- "null"
    args <- c(list(x = obj), args)
    json <- do.call(jsonlite::toJSON, args)
    return(json)
}

convCSV <- function(obj, col.names = TRUE){
    filename <- tempfile()
    write.table(obj, filename, sep = ",", na = "", col.names = col.names,
                row.names = FALSE, quote = FALSE)
    don <- readLines(filename)
    unlink(filename)
    don <- paste0(don, collapse = "\n")

    return(don)
}

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

    return(daty_s)
}

split.date.by.month <- function(start_min, end_min, tz = "Africa/Kigali"){
    daty1 <- strptime(start_min, "%Y-%m-%d %H:%M", tz = tz)
    daty2 <- strptime(end_min, "%Y-%m-%d %H:%M", tz = tz)
    datys <- seq(daty1, daty2, 'day')
    daty_m <- format(datys, "%Y%m")
    daty_m <- split(seq_along(daty_m), daty_m)
    nl <- length(daty_m)
    daty_s <- lapply(seq_along(daty_m), function(j){
        im <- daty_m[[j]]
        x <- datys[im]
        start_d <- if(j == 1) start_min else format(x[1], "%Y-%m-%d 00:00")
        end_d <- if(j == nl) end_min else format(x[length(x)], "%Y-%m-%d 23:59")
        c(start_d, end_d)
    })

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

##############################################

image.plot_Legend_pars <- function(Zmat, userOp){
    brks0 <- pretty(Zmat, n = 10, min.n = 5)
    brks0 <- if(length(brks0) > 0) brks0 else c(0, 1)
    breaks <- if(userOp$levels$custom) userOp$levels$levels else brks0
    breaks[length(breaks)] <- breaks[length(breaks)] + 1e-15

    ## legend label breaks
    legend.label <- breaks

    breaks1 <- if(userOp$levels$equidist) seq(0, 1, length.out = length(breaks)) else breaks

    Zmat <- Zmat + 1e-15
    Zrange <- if(all(is.na(Zmat))) c(0, 1) else range(Zmat, na.rm = TRUE)

    brks0 <- range(brks0)
    brks1 <- range(breaks)
    brn0 <- min(brks0[1], brks1[1])
    brn1 <- max(brks0[2], brks1[2])
    if(brn0 == breaks[1]) brn0 <- brn0 - 1
    if(brn1 == breaks[length(breaks)]) brn1 <- brn1 + 1
    if(brn0 > Zrange[1]) brn0 <- Zrange[1]
    if(brn1 < Zrange[2]) brn1 <- Zrange[2]
    breaks <- c(brn0, breaks, brn1)
    tbrks2 <- breaks1[c(1, length(breaks1))] + diff(range(breaks1)) * 0.02 * c(-1, 1)
    breaks2 <- c(tbrks2[1], breaks1, tbrks2[2])
    zlim <- range(breaks2)

    ## colors
    if(userOp$uColors$custom){
        kolFonction <- grDevices::colorRampPalette(userOp$uColors$color)
        kolor <- kolFonction(length(breaks) - 1)
    }else{
        kolFonction <- get(userOp$pColors$color, mode = "function")
        kolor <- kolFonction(length(breaks) - 1)
        if(userOp$pColors$reverse) kolor <- rev(kolor)
    }
    
    ## bin
    ## < x1; [x1, x2[; [x2, x3[; ...; [xn-1, xn]; > xn

    list(breaks = breaks,
         legend.breaks = list(zlim = zlim, breaks = breaks2),
         legend.axis = list(at = breaks1, labels = legend.label),
         colors = kolor)
}

#####

defColorKeyOptions <- function(var.name, aws = TRUE, timestep = "minute",
                               colorP = "tim.colors", reverse = FALSE,
                               customC = FALSE, colorC = NULL,
                               customL = FALSE, levels = NULL, equidist = FALSE
                             )
{
    lvl <- NULL
    if(aws){
        rrlev <- switch(timestep,
                        "minute" = c(0, 1, 2, 3, 5, 7, 10, 15, 20, 30, 50),
                        "hourly" = c(0, 1, 2, 5, 10, 15, 20, 30, 50, 70, 100),
                        "daily" = c(0, 1, 3, 5, 10, 20, 30, 50, 70, 100, 130),
                            c(0, 5, 10, 20, 30, 50, 70, 100, 150, 200, 300, 500)
                       )

        lvl <- switch(var.name,
                      "RR" = rrlev,
                      "TT" = seq(6, 36, 3),
                      "RH" = seq(45, 100, 5),
                      "PR" = seq(750, 1025, 25),
                      "RG" = seq(0, 1100, 100),
                      "FF" = c(0, 1, 2, 3, 4, 5, 6, 8, 10, 12, 15)
                     )
        customL <- TRUE
        equidist <- TRUE
    }

    op <- list(pColors = list(color = colorP, reverse = reverse),
               uColors = list(custom = customC, color = colorC),
               levels = list(custom = customL, levels = lvl, equidist = equidist)
              )

    image.plot_Legend_pars(levels, op)
}

#####

defColorKeyOptionsAcc <- function(timestep = "minute",
                                  colorP = "tim.colors", reverse = FALSE,
                                  customC = FALSE, colorC = NULL,
                                  customL = FALSE, levels = NULL, equidist = FALSE
                                 )
{
    lvl <- NULL
    lvl <- switch(timestep,
                  "hourly" = c(0, 5, 10, 20, 30, 50, 70, 100, 150, 200, 300, 500),
                  "daily" = c(0, 10, 30, 50, 70, 100, 150, 200, 300, 500, 700)
                 )
    customL <- TRUE
    equidist <- TRUE

    op <- list(pColors = list(color = colorP, reverse = reverse),
               uColors = list(custom = customC, color = colorC),
               levels = list(custom = customL, levels = lvl, equidist = equidist)
             )

    image.plot_Legend_pars(levels, op)
}

##############################################

format_Seq_Dates <- function(tstep, start, end){
    if(tstep == "hourly"){
        start <- strptime(start, "%Y-%m-%d-%H", tz = "Africa/Kigali")
        end <- strptime(end, "%Y-%m-%d-%H", tz = "Africa/Kigali")
        daty <- seq(start, end, "hour")
    }

    if(tstep == "daily"){
        start <- as.Date(start)
        end <- as.Date(end)
        daty <- seq(start, end, "day")
    }

    if(tstep == "pentad"){
        start <- as.Date(start)
        end <- as.Date(end)
        daty <- seq(start, end, "day")
        daty <- daty[as.numeric(format(daty, "%d")) <= 6]
    }

    if(tstep == "dekadal"){
        start <- as.Date(start)
        end <- as.Date(end)
        daty <- seq(start, end, "day")
        daty <- daty[as.numeric(format(daty, "%d")) <= 3]
    }

    if(tstep == "monthly"){
        start <- as.Date(paste0(paste0(strsplit(start, "-")[[1]][1:2], collapse = "-"), "-01"))
        end <- as.Date(paste0(paste0(strsplit(end, "-")[[1]][1:2], collapse = "-"), "-01"))
        daty <- seq(start, end, "month")
    }

    frmt <- switch(tstep,
                   "hourly" = "%Y%m%d%H",
                   "daily" = "%Y%m%d",
                   "pentad" = "%Y%m",
                   "dekadal" = "%Y%m",
                   "monthly" = "%Y%m")

    if(tstep %in% c("pentad", "dekadal")){
        daty <- paste0(format(daty, frmt), as.numeric(format(daty, "%d")))
    }else daty <- format(daty, frmt)

    return(daty)
}

##############################################

split_paths <- function(paths) {
    pths <- strsplit(paths, "/|\\\\")
    pths <- lapply(pths, setdiff, y = "")
    pths <- lapply(pths, rev)

    return(pths)
}

parse_aws_paths <- function(paths){
    aws_files <- sapply(paths, '[[', 1)
    aws_id <- sapply(paths, '[[', 2)
    aws_net <- sapply(paths, '[[', 4)

    data.frame(network = aws_net,
               id = aws_id,
               file = aws_files)
}
