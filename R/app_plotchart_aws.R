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
    naws <- paste(coordAWS$id, "-", coordAWS$stationName)
    npars <- if(plotrange) paste0(", Min-Ave-Max, ") else paste0(", ", pars, ", ")
    nvar <- variables.aws[variables.aws[, 1] %in% vars, 2]
    titre <- paste0(nvar, npars, naws)
    nplt <- paste(vars, pars, sep = "-")
    tstep <- coordAWS$timestep

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
