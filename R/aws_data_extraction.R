
#' Get aggregated data.
#'
#' Get aggregated data for multiple AWS.
#' 
#' @param timestep time step of the data, "hourly", "daily", "pentad", "dekadal" or "monthly"
#' @param start_time start time, format "YYYY-mm-dd HH:00"
#' @param end_time end time, format "YYYY-mm-dd HH:00"
#' @param aws_ids aws ids, a vector of aws id.\cr
#'                 Example: c("301", "306", "10050037", "000003")
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

aws_data_aggregate_ids <- function(timestep, start_time, end_time, aws_ids, aws_dir){
    out <- extract_aws_data_aggregate(
                timestep, start_time, end_time, aws_ids, aws_dir
            )

    if(is.null(out))
        out <- list(status = "no-data")

    return(convJSON(out))
}


#' Get hourly precipitation data.
#'
#' Get hourly precipitation data for all AWS.
#' 
#' @param start_time start time, format "YYYY-mm-dd HH:00"
#' @param end_time end time, format "YYYY-mm-dd HH:00"
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

aws_hourly_precip_data <- function(start_time, end_time, aws_dir){
    timestep <- "hourly"
    vars <- 'RR.Tot'
    crds <- readCoordsAWS(aws_dir)
    aws_ids <- crds$id
    data <- extract_aws_data_aggregate(
                timestep, start_time, end_time, aws_ids, aws_dir
            )
    if(is.null(data))
        return(convJSON(list(status = "no-data")))

    aws_crds <- lapply(data$info, function(x){
        data.frame(id = x$id,
                   longitude = as.numeric(x$longitude),
                   latitude = as.numeric(x$latitude),
                   stringsAsFactors = FALSE
                  )
    })
    aws_crds <- do.call(rbind, aws_crds)

    daty <- data$data[aws_crds$id][[1]]$date
    rr <- lapply(data$data[aws_crds$id], function(x) x$data[[vars]])

    inull <- sapply(rr, is.null)
    if(all(inull)) return(NULL)
    rr <- rr[!inull]
    aws_crds <- aws_crds[!inull, , drop = FALSE]
    rr <- do.call(cbind, rr)

    out <- list(coords = aws_crds, date = daty, data = rr)
    return(convJSON(out))
}


#' Get AWS minutes data.
#'
#' Get AWS variables for a list of AWS.
#' 
#' @param start_time start time, format "YYYY-mm-dd HH:MM"
#' @param end_time end time, format "YYYY-mm-dd HH:MM"
#' @param variables a vector of aws variables. Ex: c('RR', 'TT')
#' @param aws_ids aws ids, a vector of aws id
#'                 Example: c("301", "306", "10050037", "000003")
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

aws_data_variables_ids <- function(start_time, end_time, variables, aws_ids, aws_dir){
    out <- extract_aws_data_minutes(
                start_time, end_time, variables, aws_ids, aws_dir
            )

    if(is.null(out))
        out <- list(status = "no-data")

    return(convJSON(out))
}

extract_aws_data_aggregate <- function(timestep, start_time, end_time, aws_ids, aws_dir){
    crds <- readCoordsAWS(aws_dir)
    crds <- crds[crds$id %in% aws_ids, , drop = FALSE]
    crds <- split(crds, crds$id)
    crds <- lapply(crds, as.list)

    start <- strptime(start_time, "%Y-%m-%d %H:%M", tz = "Africa/Kigali")
    end <- strptime(end_time, "%Y-%m-%d %H:%M", tz = "Africa/Kigali")
    frmt <- if(timestep == "hourly") "%Y-%m-%d-%H" else "%Y-%m-%d"
    start <- format(start, frmt)
    end <- format(end, frmt)

    don <- lapply(aws_ids, function(aws){
        if(is.null(crds[[aws]])) return(NULL)
        dat <- filterAWS_AggrData(timestep, crds[[aws]]$id, start,
                                  end, crds[[aws]]$AWSGroup, aws_dir)
        if(is.null(dat)) return(NULL)

        dat[c('date', 'data')]
    })

    inull <- sapply(don, is.null)
    if(all(inull))
        return(NULL)

    don <- don[!inull]
    names(don) <- aws_ids[!inull]
    crds <- crds[aws_ids[!inull]]

    return(list(info = crds, data = don))
}

extract_aws_data_minutes <- function(start_time, end_time, variables, aws_ids, aws_dir){
    crds <- readCoordsAWS(aws_dir)
    crds <- crds[crds$id %in% aws_ids, , drop = FALSE]
    crds <- split(crds, crds$id)
    crds <- lapply(crds, as.list)

    dirAWS <- file.path(aws_dir, "PROC", "TIMESERIES", "Minutes")
    don <- lapply(aws_ids, function(aws){
        if(is.null(crds[[aws]])) return(NULL)
        fileAWS <- file.path(dirAWS, crds[[aws]]$AWSGroup, paste0(aws, ".rds"))
        readRDS(fileAWS)
    })

    start <- strptime(start_time, "%Y-%m-%d %H:%M", tz = "Africa/Kigali")
    end <- strptime(end_time, "%Y-%m-%d %H:%M", tz = "Africa/Kigali")

    don <- lapply(don, function(x){
        dat <- x$data[variables]
        inull <- sapply(dat, is.null)
        if(all(inull)) return(NULL)
        dat <- dat[!inull]

        times <- strptime(x$date, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
        ix <- times >= start & times <= end
        dat <- lapply(dat, function(y){
            v <- y[ix, , drop = FALSE]
            rownames(v) <- NULL
            v
        })

        list(time = x$date[ix], data = dat)
    })

    inull <- sapply(don, is.null)
    if(all(inull))
        return(NULL)

    don <- don[!inull]
    names(don) <- aws_ids[!inull]
    crds <- crds[aws_ids[!inull]]

    return(list(info = crds, data = don))
}
