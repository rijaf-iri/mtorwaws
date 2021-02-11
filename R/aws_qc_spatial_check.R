
#' Perform spatial check for hourly data.
#'
#' Perform quality control using spatial check method at hourly time step.
#'
#' @param start_time the start time to process, format "YYYY-MM-DD HH:00".
#'                  Example: "2019-12-15 12:00"
#' @param end_time  the end time to process, format "YYYY-MM-DD HH:00"
#' @param dirAWS full path to the directory of the parsed data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @export

qc_spatial_check <- function(start_time, end_time, dirAWS){
    tz <- "Africa/Kigali"
    pars <- list(min_aws = 10, max_dist = 50, min_nghbr = 4,
                 thres = list(TT = 2, RH = 10, PRES = 50))


    dirDATBE <- file.path(dirAWS, "PROC", "SPATIAL", "Hourly")
    dirQCBE <- file.path(dirAWS, "PROC", "QCOUT", "QC2")
    if(!dir.exists(dirQCBE))
        dir.create(dirQCBE, showWarnings = FALSE, recursive = TRUE)

    crds <- readCoordsAWS(dirAWS)

    time1 <- strptime(start_time, "%Y-%m-%d %H:%M", tz = tz)
    time2 <- strptime(end_time, "%Y-%m-%d %H:%M", tz = tz)
    seqTime <- seq(time1, time2, "hour")
    seqTime <- format(seqTime, "%Y%m%d%H")


    for(tt in seqTime){
        aws_path <- file.path(dirDATBE, paste0(tt, ".rds"))
        if(!file.exists(aws_path)) next
        aws <- try(readRDS(aws_path), silent = TRUE)
        if(inherits(aws, "try-error")) next

        ix <- match(aws$id, crds$id)
        ina <- !is.na(ix)
        ix <- ix[ina]
        if(length(ix) < pars$min_aws) next

        aws$id <- aws$id[which(ina)]
        aws$data <- aws$data[which(ina)]

        crd <- crds[ix, c("longitude", "latitude", "elevation"), drop = FALSE]
        names(crd) <- c('lon', 'lat', 'elv')
        crd <- apply(crd, 2, as.numeric)

        ## variables TT-Ave, RH-Ave, PRES-Ave
        obs <- 'TT'
        stat <- 'Ave'

        don <- getQCVariables(aws$data, crd, obs, stat)
        if(nrow(don) < pars$min_aws) next

        voisin <- lapply(seq_along(don$id), function(stn){
            istn <- seq_along(don$lon)
            crd0 <- cbind(don$lon[stn], don$lat[stn])
            crds <- do.call(cbind, don[c('lon', 'lat')])
            dist <- as.numeric(fields::rdist.earth(crd0, crds, miles = FALSE))
            io <- order(dist)
            istn <- istn[io]
            dist <- dist[io]
            idst <- dist <= pars$max_dist
            idst[istn == stn] <- FALSE
            istn <- istn[idst]
            # dist <- dist[idst]
            if(length(istn) < pars$min_nghbr) return(NULL)
            # list(id = don$id[stn], stn = c(stn, istn), dist = c(0, dist))
            list(id = don$id[stn], stn = c(stn, istn))
        })

        inull <- sapply(voisin, is.null)
        voisin <- voisin[!inull]

        #########################

        # 1st try
        flags <- lapply(voisin, function(vois){
            dat <- don[vois$stn, ]
            # ordinary or weighted least squares prediction
            z <- gstat::krige(obs~elv, locations = ~lon+lat, data = dat,
                              newdata = dat[1, , drop = FALSE], debug.level = 0)
            zdif <- abs(dat$obs[1] - z$var1.pred)

            flag <- NULL
            if(zdif > pars$thres[[obs]]){
                nl <- length(vois$stn)
                dfobs <- rep(NA, nl)
                dfobs[1] <- zdif
                for(j in 2:nl){
                    dat0 <- dat[-j, , drop = FALSE]
                    z <- gstat::krige(obs~elv, locations = ~lon+lat, data = dat0,
                                      newdata = dat[1, , drop = FALSE], debug.level = 0)
                    dfobs[j] <- abs(dat$obs[1] - z$var1.pred)
                }

                zdif <- dfobs > pars$thres[[obs]]
                if(all(zdif)){
                    flag <- vois$stn[1]
                }else{
                    if(any(!zdif)){
                        # flag <- vois$stn[!zdif]
                        # 
                        ix <- which(!zdif)
                        istn <- vois$stn[ix]
                        dfobs1 <- rep(NA, length(ix))
                        dat0 <- dat[-1, , drop = FALSE]
                        for(j in seq_along(ix)){
                            newdat <- dat[ix[j], , drop = FALSE]
                            z <- gstat::krige(obs~elv, locations = ~lon+lat, data = dat0,
                                              newdata = newdat, debug.level = 0)
                            dfobs1[j] <- abs(newdat$obs - z$var1.pred)
                        }

                        zdif1 <- dfobs1 > pars$thres[[obs]]
                        if(any(zdif1)){
                            flag <- istn[zdif1]
                        }else{
                            flag <- vois$stn[1]
                        }
                    }else{
                        flag <- vois$stn[1]
                    }
                }
            }

            return(flag)
        })

        #########################

        # 2nd try
        flags <- lapply(voisin, function(vois){
            dat <- don[vois$stn, ]
            # ordinary or weighted least squares prediction
            z <- gstat::krige(obs~elv, locations = ~lon+lat, data = dat,
                              newdata = dat, debug.level = 0)
            dfobs <- abs(dat$obs - z$var1.pred)
            zdif <- dfobs > pars$thres[[obs]]

            flag <- NULL
            if(zdif[1]){
                istn <- vois$stn[zdif][-1]
                ix <- which(zdif)[-1]


            }

            return(flag)
        })


        #########################

        flags <- unique(do.call(c, flags))


    }



}


getQCVariables <- function(data, coord, obs, stat){
    don <- lapply(lapply(data, '[[', obs), '[[', stat)
    inull <- sapply(don, is.null)
    don <- don[!inull]
    coord <- coord[!inull, , drop = FALSE]

    don <- do.call(c, don)
    id <- names(don)
    don <- data.frame(coord, obs = don, id = id)
    rownames(don) <- NULL
    don
}
