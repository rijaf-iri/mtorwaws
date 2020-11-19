#' Get AWS Status data.
#'
#' Get AWS last 24 hour data availability.
#' 
#' @param aws_dir full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

aws_data_status_24hour <- function(aws_dir){
    file_stat <- file.path(aws_dir, "STATUS", "aws_status.rds")
    aws <- readRDS(file_stat)

    return(convJSON(aws))
}

#' AWS data availability.
#'
#' Get AWS data availability for all networks 'LSI-XLOG', 'LSI-ELOG' and 'REMA'.
#' 
#' @param start_time start time, format "YYYY-mm-dd HH:MM"
#' @param end_time end time, format "YYYY-mm-dd HH:MM"
#' @param aws_dir full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

aws_data_availability_all <- function(start_time, end_time, aws_dir){
    start <- strptime(start_time, "%Y-%m-%d %H:%M", tz = "Africa/Kigali")
    end <- strptime(end_time, "%Y-%m-%d %H:%M", tz = "Africa/Kigali")

    pattern <- seq(start, end + 3600, 'hour')
    pattern <- format(pattern, "%Y%m%d%H*.rds")
    pattern <- file.path('*', 'DATA', '*', pattern)
    pattern <- file.path(aws_dir, "RAW", pattern)
    pattern <- paste('ls', pattern)

    ret_paths <- lapply(pattern, system, intern = TRUE)

    ret_paths <- lapply(ret_paths, split_paths)
    ret_paths <- lapply(ret_paths, parse_aws_paths)
    ret_paths <- do.call(rbind, ret_paths)

    aws_id <- unique(ret_paths$id)
    ret_paths$file <- gsub('\\.rds', '', ret_paths$file)
    daty <- strptime(ret_paths$file, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
    ret_paths <- ret_paths[daty >= start & daty <= end, , drop = FALSE]
    ret_paths <- split(ret_paths, ret_paths$network)
    aws_times <- lapply(ret_paths, function(n) split(n, n$id))
    aws_times <- lapply(aws_times, function(x) lapply(x, '[[', 'file'))

    crds <- readCoordsAWS(aws_dir)
    crds <- crds[crds$id %in% aws_id, , drop = FALSE]
    crds <- split(crds, crds$AWSGroup)
    crds <- lapply(crds, function(x){
        ret <- split(x, x$id)
        lapply(ret, as.list)
    })

    out <- list(info = crds, data = aws_times)
    return(convJSON(out))
}

#' AWS data availability.
#'
#' Get AWS data availability for one network.
#' 
#' @param start_time start time, format "YYYY-mm-dd HH:MM"
#' @param end_time end time, format "YYYY-mm-dd HH:MM"
#' @param aws_net aws network, 'LSI-XLOG', 'LSI-ELOG' and 'REMA'
#' @param aws_dir full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

aws_data_availability_net <- function(start_time, end_time, aws_net, aws_dir){
    start <- strptime(start_time, "%Y-%m-%d %H:%M", tz = "Africa/Kigali")
    end <- strptime(end_time, "%Y-%m-%d %H:%M", tz = "Africa/Kigali")

    pattern <- seq(start, end + 3600, 'hour')
    pattern <- format(pattern, "%Y%m%d%H*.rds")
    pattern <- file.path(aws_net, 'DATA', '*', pattern)
    pattern <- file.path(aws_dir, "RAW", pattern)
    pattern <- paste('ls', pattern)

    ret_paths <- lapply(pattern, system, intern = TRUE)

    ret_paths <- lapply(ret_paths, split_paths)
    ret_paths <- lapply(ret_paths, parse_aws_paths)
    ret_paths <- do.call(rbind, ret_paths)

    aws_id <- unique(ret_paths$id)
    ret_paths$file <- gsub('\\.rds', '', ret_paths$file)
    daty <- strptime(ret_paths$file, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
    ret_paths <- ret_paths[daty >= start & daty <= end, , drop = FALSE]
    ret_paths <- split(ret_paths[, c('id', 'file')], ret_paths$id)
    aws_times <- lapply(ret_paths, function(x) x$file)

    crds <- readCoordsAWS(aws_dir)
    crds <- crds[crds$id %in% aws_id, , drop = FALSE]
    crds <- split(crds, crds$id)
    crds <- lapply(crds, as.list)

    out <- list(info = crds, data = aws_times)
    return(convJSON(out))
}

#' AWS data availability.
#'
#' Get AWS data availability for a list of AWS.
#' 
#' @param start_time start time, format "YYYY-mm-dd HH:MM"
#' @param end_time end time, format "YYYY-mm-dd HH:MM"
#' @param aws_ids aws ids, a vector of aws id
#'                 Example: c("301", "306", "10050037", "000003")
#' @param aws_dir full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

aws_data_availability_ids <- function(start_time, end_time, aws_ids, aws_dir){
    start <- strptime(start_time, "%Y-%m-%d %H:%M", tz = "Africa/Kigali")
    end <- strptime(end_time, "%Y-%m-%d %H:%M", tz = "Africa/Kigali")
    crds <- readCoordsAWS(aws_dir)

    pattern <- seq(start, end + 3600, 'hour')
    pattern <- format(pattern, "%Y%m%d%H*.rds")

    aws_net <- crds[crds$id %in% aws_ids, c('id', 'AWSGroup')]
    aws_paths <- file.path(aws_net$AWSGroup, 'DATA', aws_net$id)
    aws_paths <- sapply(aws_paths, function(x) file.path(x, pattern))
    pattern <- file.path(aws_dir, "RAW", aws_paths)
    pattern <- paste('ls', pattern)

    ret_paths <- lapply(pattern, system, intern = TRUE)

    ret_paths <- lapply(ret_paths, split_paths)
    ret_paths <- lapply(ret_paths, parse_aws_paths)
    ret_paths <- do.call(rbind, ret_paths)

    aws_id <- unique(ret_paths$id)
    ret_paths$file <- gsub('\\.rds', '', ret_paths$file)
    daty <- strptime(ret_paths$file, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
    ret_paths <- ret_paths[daty >= start & daty <= end, , drop = FALSE]
    ret_paths <- split(ret_paths[, c('id', 'file')], ret_paths$id)
    aws_times <- lapply(ret_paths, function(x) x$file)

    crds <- crds[crds$id %in% aws_id, , drop = FALSE]
    crds <- split(crds, crds$id)
    crds <- lapply(crds, as.list)

    out <- list(info = crds, data = aws_times)
    return(convJSON(out))
}


