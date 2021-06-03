
#' Check the latest available AWS data.
#'
#' Function to check the time of the latest available data for all AWS.
#' 
#' @param dirAWS full path to the directory containing the AWS network folder \strong{"LSI-ELOG"}, \strong{"LSI-XLOG"}, \strong{"REMA"}.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA/RAW"
#' 
#' @return a data.frame with columns:
#' \itemize{ 
#' \item{\strong{Network}: }{the AWS network}
#' \item{\strong{ID}: }{the AWS ID}
#' \item{\strong{Last_Data}: }{the time of the last available data}
#' }
#' 
#' @export

check_aws_last_update <- function(dirAWS){
    info <- lapply(c("LSI-ELOG", "LSI-XLOG", "REMA"), function(awsnet){
        infopath <- file.path(dirAWS, awsnet, "INFO")
        infords <- list.files(infopath, "\\.rds$")
        infoend <- lapply(infords, function(x){
            info <- readRDS(file.path(infopath, x))
            do.call(c, info[c('id', 'end')])
        })
        infoend <- do.call(rbind, infoend)
        cbind(awsnet, infoend)
    })

    info <- do.call(rbind, info)
    info <- info[order(info[, 3]), ]
    info <- as.data.frame(info)
    names(info) <- c("Network", "ID", "Last_Data")
    tt <- strptime(info$Last_Data, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
    info$Last_Data <- format(tt, "%Y-%m-%d %H:%M")
    info
}

