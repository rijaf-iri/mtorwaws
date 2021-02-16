
#' Update AWS data availability.
#'
#' Update AWS data availability every hour.
#' 
#' @param dirAWS full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @export

update_aws_status <- function(dirAWS){
    crds <- readCoordsAWS(dirAWS)
    crds <- crds[, !names(crds) %in% "PARS"]
    ix <- is.na(crds$longitude) | is.na(crds$latitude)
    crds <- crds[!ix, , drop = FALSE]
    end_daty <- strptime(crds$end, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
    # take only station having data after 2020
    idaty <- end_daty >= as.POSIXlt("2020-01-01 00:00:00", tz = "Africa/Kigali")
    crds <- crds[idaty, , drop = FALSE]

    Sys.setenv(TZ = "Africa/Kigali")
    daty <- format(Sys.time(), "%Y%m%d%H")
    daty <- strptime(daty, "%Y%m%d%H", tz = "Africa/Kigali")

    seqHour <- seq(daty - 3600, daty, 'hour')
    sPattern <- seq(daty - 3600, daty, '10 min')

    pattern <- substr(format(sPattern, "%Y%m%d%H%M"), 1, 11)
    pattern <- paste0(pattern, ".+\\.rds$")

    perc <- lapply(seq(nrow(crds)), function(j){
        dir_aws <- file.path(dirAWS, "RAW", crds$AWSGroup[j], "DATA", crds$id[j])
        fileList <- lapply(pattern, function(p) list.files(dir_aws, p))
        fileList <- do.call(c, fileList)
        seqTime <- strptime(substr(fileList, 1, 14), "%Y%m%d%H%M%S", tz = "Africa/Kigali")

        tstep <- ifelse(is.na(crds$timestep[j]) | crds$timestep[j] < 5, 10, crds$timestep[j])
        nbstep <- 60 / tstep
        seqTime <- seqTime[seqTime > seqHour[1]]

        ctt <- cut(seqTime, seqHour, right = TRUE)
        ctt <- table(ctt)

        pr_hr <- 100 * ctt / nbstep
        pr_hr[pr_hr > 100] <- 100

        round(pr_hr, 1)
    })

    perc <- do.call(rbind, perc)
    dimnames(perc)[[2]] <- NULL
    colAWS <- c("id", "stationName", "longitude", "latitude",
                "AWSGroup", "start", "end", "timestep")
    coords <- crds[, colAWS]

    status_file <- file.path(dirAWS, "STATUS", "aws_status.rds")
    aws <- readRDS(status_file)
    aws$time <- c(aws$time[-1], seqHour[length(seqHour)])
    ix <- match(coords$id, aws$coords$id)
    aws$status <- cbind(aws$status[ix, -1], perc)
    aws$status[is.na(aws$status)] <- 0
    aws$coords <- coords
    aws$actual_time <- daty
    aws$updated <- Sys.time()

    saveRDS(aws, file = status_file)
}

###############################
## create aws status for one month
create_aws_status_month <- function(dirAWS){
    crds <- readCoordsAWS(dirAWS)
    crds <- crds[, !names(crds) %in% "PARS"]
    ix <- is.na(crds$longitude) | is.na(crds$latitude)
    crds <- crds[!ix, , drop = FALSE]
    end_daty <- strptime(crds$end, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
    # take only station having data after 2020
    idaty <- end_daty >= as.POSIXlt("2020-01-01 00:00:00", tz = "Africa/Kigali")
    crds <- crds[idaty, , drop = FALSE]

    Sys.setenv(TZ = "Africa/Kigali")
    daty <- format(Sys.time(), "%Y%m%d%H")
    daty <- strptime(daty, "%Y%m%d%H", tz = "Africa/Kigali")

    seqHour <- seq(daty - 2592000, daty, 'hour')
    sPattern <- seq(daty - 2592000, daty, 'day')

    pattern <- substr(format(sPattern, "%Y%m%d"), 1, 11)
    pattern <- paste0(pattern, ".+\\.rds$")

    perc <- lapply(seq(nrow(crds)), function(j){
        dir_aws <- file.path(dirAWS, "RAW", crds$AWSGroup[j], "DATA", crds$id[j])
        fileList <- lapply(pattern, function(p) list.files(dir_aws, p))
        fileList <- do.call(c, fileList)
        seqTime <- strptime(substr(fileList, 1, 14), "%Y%m%d%H%M%S", tz = "Africa/Kigali")

        tstep <- ifelse(is.na(crds$timestep[j]) | crds$timestep[j] < 5, 10, crds$timestep[j])
        nbstep <- 60 / tstep
        seqTime <- seqTime[seqTime > seqHour[1]]

        ctt <- cut(seqTime, seqHour, right = TRUE)
        ctt <- table(ctt)

        pr_hr <- 100 * ctt / nbstep
        pr_hr[pr_hr > 100] <- 100

        round(pr_hr, 1)
    })

    perc <- do.call(rbind, perc)
    dimnames(perc) <- NULL
    perc[is.na(perc)] <- 0

    colAWS <- c("id", "stationName", "longitude", "latitude",
                "AWSGroup", "start", "end", "timestep")
    coords <- crds[, colAWS]

    status_file <- file.path(dirAWS, "STATUS", "aws_status.rds")

    aws <- list(coords = coords, time = seqHour[-1], status = perc,
                actual_time = daty, updated = Sys.time())

    saveRDS(aws, file = status_file)
}
