
#' Checking AWS variables.
#'
#' Get AWS variables for a list of AWS.
#' 
#' @param start_time start time, format "YYYY-mm-dd HH:MM"
#' @param end_time end time, format "YYYY-mm-dd HH:MM"
#' @param aws_ids aws ids, a vector of aws id
#'                 Example: c("301", "306", "10050037", "000003")
#' @param variables a vector of aws variables. Ex: c('RR', 'TT')
#' @param aws_dir full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

aws_check_variables_ids <- function(start_time, end_time, aws_ids, variables, aws_dir){
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
    
    ret_paths <- do.call(c, ret_paths)
    tmp <- split_paths(ret_paths)
    tmp <- parse_aws_paths(tmp)
    tmp$file <- gsub('\\.rds', '', tmp$file)
    daty <- strptime(tmp$file, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
    
    ix <- daty >= start & daty <= end
    tmp <- tmp[ix, , drop = FALSE]
    ret_paths <- ret_paths[ix]

    if(length(ret_paths) == 0)
        return(convJSON(list(status = "no-data")))

    vars <- lapply(ret_paths, function(pth){
        x <- readRDS(pth)
        v <- x$data[variables]
        inull <- sapply(v, is.null)
        as.integer(!inull)
    })

    vars <- do.call(rbind, vars)
    aws_id <- tmp$id
    tmp <- data.frame(tmp$file, vars)
    names(tmp) <- c('Time', variables)
    tmp <- split(tmp, aws_id)

    crds <- crds[crds$id %in% aws_id, , drop = FALSE]
    crds <- split(crds, crds$id)
    crds <- lapply(crds, as.list)

    out <- list(info = crds, data = tmp)
    return(convJSON(out))
}
