
readAWSMinuteData <- function(aws, aws_dir){
    crds <- readCoordsAWS(aws_dir)
    iaws <- which(crds$id == aws)
    info <- as.list(crds[iaws, ])

    file.rds <- file.path(aws_dir, "PROC", "TIMESERIES", "Minutes",
                          info$AWSGroup, paste0(aws, ".rds"))
    don <- readRDS(file.rds)
    return(c(info, don))
}

readAWS_AggrData <- function(tstep, aws, aws_net, aws_dir){
    dirSTEP <- switch(tstep,
                    "hourly" = "Hourly",
                    "daily" = "Daily",
                    "pentad" = "Pentadal",
                    "dekadal" = "Dekadal",
                    "monthly" = "Monthly"
                  )
    
    AWS_DIRaggr <- file.path(aws_dir, "PROC", "TIMESERIES", dirSTEP, aws_net)
    aws.file <- file.path(AWS_DIRaggr, paste0(aws, ".rds"))
    don <- NULL
    if(file.exists(aws.file)) don <- readRDS(aws.file)

    return(don)
}

filterAWS_AggrData <- function(tstep, aws, start, end, aws_net, aws_dir){
    don <- readAWS_AggrData(tstep, aws, aws_net, aws_dir)
    if(is.null(don)) return(NULL)

    datyTs <- switch(tstep,
                    'hourly' = local({
                        xstart <- strptime(start, "%Y-%m-%d-%H", tz = "Africa/Kigali")
                        xend <- strptime(end, "%Y-%m-%d-%H", tz = "Africa/Kigali")
                        tt <- seq(xstart, xend, 'hour')
                        tt1 <- format(tt, "%Y%m%d%H")
                        list(time = tt, date = tt1)
                    }),
                    'daily' = local({
                        xstart <- as.Date(start, "%Y-%m-%d")
                        xend <- as.Date(end, "%Y-%m-%d")
                        tt <- seq(xstart, xend, 'day')
                        tt1 <- format(tt, "%Y%m%d")
                        list(time = tt, date = tt1)
                    }),
                    'pentad' = local({
                        xstart <- as.Date(start, "%Y-%m-%d")
                        xend <- as.Date(end, "%Y-%m-%d")
                        tt <- seq(xstart, xend, 'day')
                        tmp <- as.numeric(format(tt, '%d'))
                        ix <-  tmp < 7
                        tt1 <- paste0(format(tt, "%Y%m"), tmp)[ix]
                        it <- c(3, 7, 13, 17, 23, 27)[tmp[ix]]
                        tt <- as.Date(paste0(format(tt, "%Y-%m-")[ix], it))
                        list(time = tt, date = tt1)
                    }),
                    'dekadal' = local({
                        xstart <- as.Date(start, "%Y-%m-%d")
                        xend <- as.Date(end, "%Y-%m-%d")
                        tt <- seq(xstart, xend, 'day')
                        tmp <- as.numeric(format(tt, '%d'))
                        ix <-  tmp < 4
                        tt1 <- paste0(format(tt, "%Y%m"), tmp)[ix]
                        it <- c(5, 15, 25)[tmp[ix]]
                        tt <- as.Date(paste0(format(tt, "%Y-%m-")[ix], it))
                        list(time = tt, date = tt1)
                    }),
                    'monthly' = local({
                        xstart <- as.Date(paste0(start, '-15'), "%Y-%m-%d")
                        xend <- as.Date(paste0(end, '-15'), "%Y-%m-%d")
                        tt <- seq(xstart, xend, 'month')
                        tt1 <- format(tt, "%Y%m")
                        list(time = tt, date = tt1)
                    })
                )
    it <- match(datyTs$date, don$date)
    if(all(is.na(it))) return(NULL)
    don <- lapply(don$data, function(x) x[it, , drop = FALSE])
    don <- do.call(cbind.data.frame, don)
    nom <- names(don)
    if("Tot" %in% nom){
        nom[nom == "Tot"] <- "RR.Tot"
        names(don) <- nom
    }
    if(all(is.na(don))) return(NULL)
    return(list(time = datyTs$time, date = datyTs$date, data = don))
}
