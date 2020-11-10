
#' Read AWS coordinates.
#'
#' Read AWS coordinates.
#' 
#' @param aws_dir full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

readCoords <- function(aws_dir){
    coordAWS <- readCoordsAWS(aws_dir)
    coordAWS[is.na(coordAWS)] <- ""
    return(convJSON(coordAWS))
}

#############
#' Get AWS coordinates.
#'
#' Get AWS coordinates to display on map.
#' 
#' @param aws_dir full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

readCoordsMap <- function(aws_dir){
    crds <- readCoordsAWS(aws_dir)
    pars <- crds$PARS
    crds <- crds[, !names(crds) %in% "PARS"]

    #############
    xcrd <- crds[, c('longitude', 'latitude')]
    xcrd <- paste(xcrd[, 1], xcrd[, 2], sep = "_")
    ix1 <- duplicated(xcrd) & !is.na(crds$longitude)
    ix2 <- duplicated(xcrd, fromLast = TRUE) & !is.na(crds$longitude)
    ix <- ix1 | ix2
    icrd <- unique(xcrd[ix])

    #############

    crds <- apply(crds, 2, as.character)
    crds <- cbind(crds, StatusX = "blue")

    #############
    if(length(icrd) > 0){
        for(jj in icrd){
            ic <- xcrd == jj
            xx <- apply(crds[ic, ], 2, paste0, collapse = " | ")
            xx <- matrix(xx, nrow = 1, dimnames = list(NULL, names(xx)))
            xx <- do.call(rbind, lapply(seq_along(which(ic)), function(i) xx))

            xcr <- crds[ic, c('longitude', 'latitude')]
            crds[ic, ] <- xx
            crds[ic, c('longitude', 'latitude')] <- xcr
            crds[ic, 'StatusX'] <- "orange"
        }
    }

    #############
    crds[is.na(crds)] <- ""
    crds <- cbind(crds, LonX = crds[, 3], LatX = crds[, 4])
    ix <- crds[, 'LonX'] == "" | crds[, 'LatX'] == ""
    crds[ix, c('LonX', 'LatX')] <- NA
    crds <- as.data.frame(crds)
    crds$LonX <- as.numeric(as.character(crds$LonX))
    crds$LatX <- as.numeric(as.character(crds$LatX))
    crds$PARS <- pars

    return(convJSON(crds))
}

#############
#' Get AWS Wind coordinates.
#'
#' Get AWS Wind coordinates to display on map.
#' 
#' @param aws_dir full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

readAWSWind <- function(aws_dir){
    coordAWS <- readCoordsAWS(aws_dir)
    coordAWS[is.na(coordAWS)] <- ""
    iwnd <- sapply(coordAWS$PARS, function(x){
        all(c('DD', 'FF') %in% x)
    })
    coordAWS <- coordAWS[iwnd, c('id', 'stationName', 'AWSGroup', 'timestep')]

    return(convJSON(coordAWS))
}

#############
#' Read AWS Info file.
#'
#' Read AWS Info file.
#'
#' @param aws the AWS ID
#' @param aws_net the AWS network, "LSI-ELOG", "LSI-XLOG" or "REMA". 
#' @param aws_dir full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

readAWSInfo <- function(aws, aws_net, aws_dir){
    dirINFO <- file.path(aws_dir, "RAW", aws_net, "INFO")
    fileInfo <- file.path(dirINFO, paste0(aws, ".rds"))
    info <- readRDS(fileInfo)

    if(aws_net == "REMA"){
        wnd <- c('FF', 'FFmax')
        if(all(wnd %in% names(info$vars))){
            info$vars$FF <- c("Ave", "Max")
            info$vars <- info$vars[!names(info$vars) %in% 'FFmax']
        }
        if('FFmax' %in% names(info$vars)){
            nom <- names(info$vars)
            nom[nom == "FFmax"] <- "FF"
            names(info$vars) <- nom
        }
    }

    return(convJSON(info))
}

#############
#' Get AWS Status data.
#'
#' Get AWS Status data to display on map.
#' 
#' @param hour an integer, the last hour to display.
#' @param aws_dir full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

readAWSStatus <- function(hour, aws_dir){
    file_stat <- file.path(aws_dir, "STATUS", "aws_status.rds")
    aws <- readRDS(file_stat)
    hour <- as.numeric(hour)
    if(hour > 1){
        ic <- (24 - hour + 1):24
        stat <- aws$status[, ic]
        stat <- rowMeans(stat, na.rm = TRUE)
    }else stat <- aws$status[, 24]

    crds <- aws$coords
    crds$Availability <- paste(round(stat, 1), "%")
    kol <- cut(stat, c(0, 25, 50, 75, 100), labels = c('orange','yellow','green','blue'))
    kol <- as.character(kol)
    kol <- ifelse(is.na(kol), 'red', kol)
    crds$StatusX <- kol
    crds$longitude <- as.numeric(crds$longitude)
    crds$latitude <- as.numeric(crds$latitude)
    crds$start <- as.character(strptime(crds$start, "%Y%m%d%H%M%S", tz = "Africa/Kigali"))
    crds$end <- as.character(strptime(crds$end, "%Y%m%d%H%M%S", tz = "Africa/Kigali"))

    aws <- list(data = crds, time = aws$actual_time, update = aws$updated)
    return(convJSON(aws))
}
