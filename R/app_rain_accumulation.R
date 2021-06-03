
#' Compute precipitation accumulation.
#'
#' Compute precipitation accumulation for chart display.
#' 
#' @param tstep time basis to accumulate the data.
#' @param aws AWS ID.
#' @param start start date.
#' @param end end date.
#' @param accumul accumulation duration.
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

chartRainAccumulAWS <- function(tstep, aws, start, end, accumul, aws_dir)
{
    ts <- list(data = NULL, status = 'no-data', name = 'none')
    don <- tsRainAccumulAWS(tstep, aws, start, end, accumul, aws_dir)

    naws <- paste(don$coords$id, "-", don$coords$stationName)
    tt <- switch(tstep, "hourly" = "Hour", "daily" = "Day")
    titre <- paste(accumul, tt, "Rain Accumulation", "_", naws)
    nplt <- "RR_Accumul"

    if(!is.null(don$data)){
        time <- 1000 * as.numeric(as.POSIXct(don$date))
        ts <- as.matrix(cbind(time, don$data))
        dimnames(ts) <- NULL
        ts <- list(data = ts, status = 'plot', name = nplt)
    }

    Opts <- list(title = titre, status = ts$status,
                 name = ts$name, filename = gsub(" ", "", gsub(",", "_", titre))
               )
    ret <- list(opts = Opts, data = ts$data)

    return(convJSON(ret))
}

################
#' Compute precipitation accumulation.
#'
#' Compute precipitation accumulation for spatial display.
#' 
#' @param tstep time basis to accumulate the data.
#' @param time target date.
#' @param accumul accumulation duration.
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

displayMAPRainAccumul <- function(tstep, time, accumul, aws_dir)
{
    spdon <- spRainAccumulAWS(tstep, time, accumul, aws_dir)
    if(spdon$status == "no-data") return(convJSON(spdon))
    don <- spdon$data

    colorC <- RColorBrewer::brewer.pal(n = 9, name = "YlGnBu")
    ops <- list(timestep = tstep, customC = TRUE, colorC = colorC)
    ix <- !is.na(don$accumul) & don$accumul == 0
    pars <- do.call(defColorKeyOptionsAcc, ops)

    zmin <- suppressWarnings(min(don$accumul, na.rm = TRUE))
    if(!is.infinite(zmin)){
        pars$breaks[1] <- ifelse(pars$breaks[1] > zmin, zmin, pars$breaks[1])
    }
    zmax <- suppressWarnings(max(don$accumul, na.rm = TRUE))
    if(!is.infinite(zmax)){
        nl <- length(pars$breaks)
        pars$breaks[nl] <- ifelse(pars$breaks[nl] < zmax, zmax, pars$breaks[nl])
    }

    kolor.p <- pars$colors[findInterval(don$accumul, pars$breaks, rightmost.closed = TRUE, left.open = TRUE)]
    kolor.p[ix] <- "#FFFFFF"

    nom <- gsub('\\.', '', names(pars))
    names(pars) <- nom

    ##########
    pars <- list(labels = pars$legendaxis$labels, colors = pars$colors)
    ##########

    don <- list(date = spdon$date, data = don, color = kolor.p,
                key = pars, status = spdon$status)

    return(convJSON(don))
}

################

tsRainAccumulAWS <- function(tstep, aws, start, end, accumul, aws_dir)
{
    accumul <- as.numeric(accumul)
    coordAWS <- readCoordsAWS(aws_dir)
    coordAWS <- as.list(coordAWS[coordAWS$id == aws, ])
    aws_net <- coordAWS$AWSGroup

    out <- list(data = NULL, date = NULL, coords = coordAWS)

    don <- filterAWS_AggrData(tstep, aws, start, end, aws_net, aws_dir)
    if(is.null(don)) return(out)
    if(!"RR.Tot" %in% names(don$data)) return(out)

    x <- don$data[["RR.Tot"]]
    if(accumul > 1){
        pth_pars <- file.path(aws_dir, "PARAMS", "Rolling_Aggr.json")
        pars <- jsonlite::fromJSON(pth_pars)
        aggr_pars <- list(win = accumul, fun = 'sum', na.rm = TRUE,
                          min.data = as.numeric(pars$minfrac) * accumul,
                          na.pad = TRUE, fill = FALSE, align = "right"
                        )
        x <- do.call(.rollfun.vec, c(list(x = x), aggr_pars))
    }

    list(data = x, date = don[["time"]], coords = coordAWS)
}

spRainAccumulAWS <- function(tstep, time, accumul, aws_dir){
    accumul <- as.numeric(accumul)
    infoData <- switch(tstep,
                       'hourly' = local({
                           dirA <- "Hourly"
                           end <- strptime(time, "%Y-%m-%d-%H", tz = "Africa/Kigali")
                           start <- end - (accumul - 1) * 3600
                           tt <- seq(start, end, 'hour')
                           tt1 <- format(tt, "%Y%m%d%H")
                           list(time = tt, date = tt1, dir = dirA)
                       }),
                       'daily' = local({
                           dirA <- "Daily"
                           end <- as.Date(time, "%Y-%m-%d")
                           start <- end - accumul + 1
                           tt <- seq(start, end, 'day')
                           tt1 <- format(tt, "%Y%m%d")
                           list(time = tt, date = tt1, dir = dirA)
                       })
                    )


    dirDAT <- file.path(aws_dir, "PROC", "SPATIAL", infoData$dir)
    timefiles <- file.path(dirDAT, paste0(infoData$date, ".rds"))
    dout <- as.character(infoData$time[accumul])

    data.null <- list(date = dout, data = "null", status = "no-data")
    if(all(!file.exists(timefiles))) return(data.null)

    don <- lapply(timefiles, function(fl){
        if(!file.exists(fl)) return(list(id = NULL, data = NULL))
        x <- readRDS(fl)
        y <- lapply(lapply(x$data, '[[', 'RR'), '[[', 'Tot')
        inull <- sapply(y, is.null)
        if(all(inull)) x$data <- NULL
        y[inull] <- NA
        x$data <- do.call(c, y)
        return(x)
    })

    dat <- lapply(don, "[[", "data")
    id <- lapply(don, "[[", "id")

    inull <- sapply(dat, is.null)
    if(all(inull)) return(data.null)

    if(accumul > 1){
        ID <- unique(do.call(c, id))
        dat <- lapply(seq_along(dat), function(j){
            if(is.null(dat[[j]])) return(rep(NA, length(ID)))
            ix <- match(ID, id[[j]])
            dat[[j]][ix]
        })
        dat <- do.call(rbind, dat)

        pth_pars <- file.path(aws_dir, "PARAMS", "Rolling_Aggr.json")
        pars <- jsonlite::fromJSON(pth_pars)
        ina <- colSums(!is.na(dat), na.rm = TRUE)
        ina <- ina < as.numeric(pars$minfrac) * accumul
        x <- unname(colSums(dat, na.rm = TRUE))
        x[ina] <- NA
    }else{
        ID <- id[[1]]
        x <- unname(dat[[1]])
    }

    if(all(is.na(x))) return(data.null)

    coordAWS <- readCoordsAWS(aws_dir)
    ix <- match(ID, coordAWS$id)
    nom <- c("id", "stationName", "longitude", "latitude", "elevation", "AWSGroup")
    coordAWS <- coordAWS[ix, nom, drop = FALSE]

    if(nrow(coordAWS) == 0) return(data.null)

    coordAWS$longitude <- as.numeric(as.character(coordAWS$longitude))
    coordAWS$latitude <- as.numeric(as.character(coordAWS$latitude))
    coordAWS$elevation <- as.numeric(as.character(coordAWS$elevation))

    coordAWS$accumul <- x
    list(date = dout, data = coordAWS, status = "ok")
}
