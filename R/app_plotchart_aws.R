#' Get 10 minutes data.
#'
#' Get 10 minutes data to display on chart.
#' 
#' @param aws AWS ID.
#' @param vars variables.
#' @param pars parameters.
#' @param start start time.
#' @param end end time.
#' @param plotrange get range.
#' @param aws_net AWS network.
#' @param aws_dir full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

chartMinAWSData <- function(aws, vars, pars, start, end,
                            plotrange, aws_net, aws_dir)
{
    plotrange <- as.logical(as.integer(plotrange))
    coordAWS <- readCoordsAWS(aws_dir)
    coordAWS <- as.list(coordAWS[coordAWS$id == aws, ])
    # aws_net <- coordAWS$AWSGroup
    naws <- paste(coordAWS$id, "-", coordAWS$stationName)
    tstep <- coordAWS$timestep

    ############
    npars <- if(plotrange) paste0(", Min-Ave-Max, ") else paste0(", ", pars, ", ")
    nvar <- variables.aws[variables.aws[, 1] %in% vars, 2]
    titre <- paste0(nvar, npars, naws)
    nplt <- paste(vars, pars, sep = "-")

    OUT <- list(opts = list(title = titre, arearange = FALSE, status = 'no-data', name = 'none',
                filename = gsub(" ", "", gsub(",", "_", titre))), data = NULL, var = vars)

    ############
    awsfile <- file.path(aws_dir, "PROC", "TIMESERIES", "Minutes", aws_net, paste0(aws, ".rds"))
    don <- readRDS(awsfile)
    daty <- strptime(don[["date"]], "%Y%m%d%H%M%S", tz = "Africa/Kigali")

    plotR <- FALSE
    don <- don$data[[vars]]

    if(is.null(don)) return(convJSON(OUT))

    don <- don[, !duplicated(names(don)), drop = FALSE]
    rvars <- c("Min", "Max", "Ave")
    if(all(rvars %in% names(don)) & plotrange){
        plotR <- TRUE
        don <- don[, rvars]
    }else don <- don[[pars]]

    if(is.null(don)) return(convJSON(OUT))

    start <- strptime(start, "%Y-%m-%d-%H-%M", tz = "Africa/Kigali")
    end <- strptime(end, "%Y-%m-%d-%H-%M", tz = "Africa/Kigali")
    ix <- daty >= start & daty <= end

    if(any(ix)){
        daty <- daty[ix]
        don <- if(plotR) don[ix, , drop = FALSE] else don[ix]
        if(length(daty) > 1){
            ddif <- diff(daty)
            idf <- ddif > tstep
            if(any(idf)){
                idt <- which(idf)
                miss.daty <- daty[idt] + tstep * 60
                miss.daty <- format(miss.daty, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
                don1 <- daty1 <- rep(NA, length(daty) + length(miss.daty))
                if(plotR) don1 <- data.frame(Min = don1, Max = don1, Ave = don1)
                daty1[idt + seq(length(miss.daty))] <- miss.daty
                ix <- is.na(daty1)
                daty1[ix] <- format(daty, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
                if(plotR){
                    don1[ix, ] <- don
                }else{
                    don1[ix] <- don
                }
                daty <- strptime(daty1, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
                don <- don1
            }
        }

        ## convert to millisecond
        time <- 1000 * as.numeric(as.POSIXct(daty))

        if(plotR){
            don <- as.matrix(cbind(time, don[, c('Min', 'Max', 'Ave')]))
            dimnames(don) <- NULL

            OUT$data <- don
            OUT$opts$name <- c("Range", "Average")
        }else{
            don <- as.matrix(cbind(time, don))
            dimnames(don) <- NULL

            OUT$data <- don
            OUT$opts$name <- nplt
        }

        OUT$opts$arearange <- plotR
        OUT$opts$status <- 'plot'
    }

    return(convJSON(OUT))
}

##########
#' Get aggregated data.
#'
#' Get aggregated data to display on chart.
#' 
#' @param tstep time step.
#' @param aws AWS ID.
#' @param vars variables.
#' @param pars parameters.
#' @param start start date.
#' @param end end date.
#' @param plotrange get range.
#' @param aws_net AWS network.
#' @param aws_dir full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

chartAggrAWSData <- function(tstep, aws, vars, pars, start,
                             end, plotrange, aws_net, aws_dir)
{
    plotrange <- as.logical(as.integer(plotrange))
    coordAWS <- readCoordsAWS(aws_dir)
    coordAWS <- as.list(coordAWS[coordAWS$id == aws, ])
    # aws_net <- coordAWS$AWSGroup
    naws <- paste(coordAWS$id, "-", coordAWS$stationName)
 
    npars <- if(plotrange) paste0(", Min-Ave-Max, ") else paste0(", ", pars, ", ")
    nvar <- variables.aws[variables.aws[, 1] %in% vars, 2]
    titre <- paste0(nvar, npars, naws)
    nplt <- paste(vars, "-", pars)

    ts <- list(data = NULL, status = 'no-data', name = 'none')

    don <- filterAWS_AggrData(tstep, aws, start, end, aws_net, aws_dir)
    plotR <- FALSE

    if(!is.null(don)){
        time <- 1000 * as.numeric(as.POSIXct(don[["time"]]))
        if(vars == "RR"){
            vnm <- "RR.Tot"
            if(vnm %in% names(don$data)){
                ts <- as.matrix(cbind(time, don$data[[vnm]]))
                dimnames(ts) <- NULL
                ts <- list(data = ts, status = 'plot', name = nplt)
            }
        }else{
            vnm <- paste0(vars, ".", c("Min", "Max", "Ave"))
            ix <- match(vnm, names(don$data))
            ii <- which(!is.na(ix))
            if(length(ii) > 0){
                ts <- matrix(NA, ncol = 3, nrow = length(time))
                ts[, ii] <- as.matrix(don$data[, ix[ii], drop = FALSE])

                if(plotrange){
                    if(all(is.na(ts))){
                        ts <- list(data = NULL, status = 'no-data', name = 'none')
                    }else{
                        ts <- cbind(time, ts)
                        dimnames(ts) <- NULL
                        ts <- list(data = ts, status = 'plot', name = c("Range", "Average"))
                        plotR <- TRUE
                    }
                }else{
                    nom <- c("Min", "Max", "Ave")
                    ts <- ts[, nom %in% pars]
                    if(all(is.na(ts))){
                        ts <- list(data = NULL, status = 'no-data', name = 'none')
                    }else{
                        ts <- cbind(time, ts)
                        dimnames(ts) <- NULL
                        ts <- list(data = ts, status = 'plot', name = nplt)
                    }
                }
            }
        }
    }

    Opts <- list(title = titre, arearange = plotR, status = ts$status,
                 name = ts$name, filename = gsub(" ", "", gsub(",", "_", titre))
               )
    ret <- list(opts = Opts, data = ts$data, var = vars)

    return(convJSON(ret))
}

##########
#' Get aggregated data.
#'
#' Get aggregated data to display on table.
#' 
#' @param tstep time step.
#' @param aws AWS ID.
#' @param start start date.
#' @param end end date.
#' @param aws_net AWS network.
#' @param aws_dir full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

displayTableAgrrAWS <- function(tstep, aws, start, end, aws_net, aws_dir){
    don0 <- data.frame(Date = NA, Status = "no.data")
    don <- filterAWS_AggrData(tstep, aws, start, end, aws_net, aws_dir)
    if(!is.null(don)){
        don1 <- round(don$data, 1)
        daty1 <- don$date
        ina <- rowSums(!is.na(don1)) > 0
        if(any(ina)){
            don0 <- don1[ina, , drop = FALSE]
            daty0 <- daty1[ina]
            don0[is.na(don0)] <- -99
            don0 <- data.frame(Date = daty0, don0)
            rownames(don0) <- NULL
        }
    }

    return(convJSON(don0))
}

##########
#' Get aggregated data.
#'
#' Get aggregated data to display on chart for multiple AWS.
#' 
#' @param tstep time step.
#' @param aws vector of AWS IDs.
#' @param vars variables.
#' @param pars parameters.
#' @param start start time.
#' @param end end time.
#' @param aws_dir full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

chartAggrAWSDataSel <- function(tstep, aws, vars, pars, start, end, aws_dir){
    coordAWS <- readCoordsAWS(aws_dir)
    iaws <- match(aws, coordAWS$id)
    coordAWS <- as.list(coordAWS[iaws, , drop = FALSE])
    aws_net <- coordAWS$AWSGroup
    aws_name <- coordAWS$stationName
    vnm <- paste0(vars, '.', pars)
    varlab <- paste0(vars, '-', pars)

    filename <- paste0(tstep, '_', vars, '-', pars)
    varname <- variables.aws[variables.aws[, 1] %in% vars, 2]
    parname <- switch(pars, "Ave" = "Average", "Tot" = "Total",
                            "Min" = "Minimum", "Max" = "Maximum")
    titre <- paste(varname, "-", parname)
    opts <- list(filename = filename, status = "plot", title = titre)

    kolor <- fields::tim.colors(length(aws))

    don <- lapply(seq_along(aws), function(j){
        dat <- filterAWS_AggrData(tstep, aws[j], start, end, aws_net[j], aws_dir)
        if(is.null(dat)) return(NULL)

        time <- 1000 * as.numeric(as.POSIXct(dat[["time"]]))
        dat <- as.matrix(cbind(time, dat$data[[vnm]]))
        dimnames(dat) <- NULL

        list(name = paste(aws[j], '-', aws_name[j]),
            color = kolor[j],
            data = dat)
    })

    inull <- sapply(don, is.null)
    if(all(inull)){
        opts$status <- "no-data"
        don <- NULL
    }else{
        don <- don[!inull]
    }
    out <- list(data = don, opts = opts, var = varlab)

    return(convJSON(out))
}

##########
#' Get aggregated data.
#'
#' Get aggregated data to display on table for multiple AWS.
#' 
#' @param tstep time step.
#' @param aws vector of AWS IDs.
#' @param vars variables.
#' @param pars parameters.
#' @param start start time.
#' @param end end time.
#' @param aws_dir full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

displayTableAgrrDataSel <- function(tstep, aws, vars, pars, start, end, aws_dir){
    coordAWS <- readCoordsAWS(aws_dir)
    iaws <- match(aws, coordAWS$id)
    coordAWS <- as.list(coordAWS[iaws, , drop = FALSE])
    aws_net <- coordAWS$AWSGroup
    vnm <- paste0(vars, '.', pars)
    varname <- variables.aws[variables.aws[, 1] %in% vars, 2]
    parname <- switch(pars, "Ave" = "Average", "Tot" = "Total",
                            "Min" = "Minimum", "Max" = "Maximum")
    titre <- paste(varname, "-", parname)

    don <- lapply(seq_along(aws), function(j){
        dat <- filterAWS_AggrData(tstep, aws[j], start, end, aws_net[j], aws_dir)
        if(is.null(dat)) return(NULL)

        list(date = dat[["date"]], data = dat$data[[vnm]])
    })

    inull <- sapply(don, is.null)
    don0 <- data.frame(Date = NA, Status = "no.data")
    if(!all(inull)){
        don <- don[!inull]
        aws <- aws[!inull]

        daty <- don[[1]]$date
        don <- do.call(cbind, lapply(don, '[[', 'data'))
        ina <- colSums(!is.na(don)) > 0
        if(any(ina)){
            don0 <- data.frame(daty, don[, ina, drop = FALSE])
            don0[is.na(don0)] <- -99
            names(don0) <- c("Date", aws[ina])
            rownames(don0) <- NULL
        }
    }
    out <- list(data = don0, title = titre, order = names(don0))

    return(convJSON(out))
}

#################

chartMinAWSDataSel <- function(aws, vars, pars, start, end, aws_dir){
    coordAWS <- readCoordsAWS(aws_dir)
    iaws <- match(aws, coordAWS$id)
    coordAWS <- as.list(coordAWS[iaws, , drop = FALSE])

    dirAWS <- file.path(aws_dir, "PROC", "TIMESERIES", "Minutes")
    fileAWS <- file.path(dirAWS, coordAWS$AWSGroup, paste0(aws, ".rds"))
    don <- lapply(fileAWS, readRDS)

    daty <- lapply(don, '[[', 'date')
    don <- lapply(don, function(x) x[['data']][[vars]][[pars]])
 
    daty <- lapply(daty, strptime, format = "%Y%m%d%H%M%S", tz = "Africa/Kigali")
    start <- strptime(start, "%Y-%m-%d-%H-%M", tz = "Africa/Kigali")
    end <- strptime(end, "%Y-%m-%d-%H-%M", tz = "Africa/Kigali")

    index <- lapply(daty, function(ix) ix >= start & ix <= end)
    dat <- lapply(seq_along(index), function(j){
        list(date = daty[[j]][index[[j]]], data = don[[j]][index[[j]]])
    })


}