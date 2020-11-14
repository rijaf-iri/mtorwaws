##########
#' Read log files.
#'
#' Read log files to be displayed.
#' 
#' @param logtype type of log files: "logperaws", "logproc" or "logupload".
#' @param date date to read.
#' @param aws_net AWS network: "REMA", "LSI-XLOG" or "LSI-ELOG".
#' @param aws_dir full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return JSON object
#' 
#' @export

displayLogFiles <- function(logtype, aws_dir, aws_net = NULL, date = NULL){
    out <- list(status = "no-data", log = NULL, update = NULL)
    if(logtype == "logupload"){
        logfile <- file.path(aws_dir, "RAW", aws_net, "LOG", "UPLOAD_LOG.txt")
        if(!file.exists(logfile)) return(convJSON(out))
        x <- readLines(logfile, skipNul = TRUE)

        sep <- grep('\\*\\*\\*.+', x)
        start <- c(1, sep[-length(sep)] + 1)
        end <- sep - 1

        ret <- lapply(seq_along(start), function(j){
            paste0(x[start[j]:end[j]], collapse = "\n")
        })
        ret <- do.call(c, ret)

        out$log <- rev(ret)
        out$status <- "ok"
    }else if(logtype == "logproc"){
        logdir <- file.path(aws_dir, "RAW", aws_net, "LOG")
        daty <- as.Date(date, "%Y-%m-%d")
        today <- Sys.Date()
        if(format(daty, "%m") == format(today, "%m")){
            logfile <- "AWS_LOG.txt"
        }else{
            logfile <- paste0("AWS_LOG_", format(daty, "%Y%m"), ".txt")
        }
        logpath <- file.path(logdir, logfile)
        if(!file.exists(logpath)) return(convJSON(out))
        x <- readLines(logpath, skipNul = TRUE)

        sep <- grep('\\*\\*\\*.+', x)
        start <- c(1, sep[-length(sep)] + 1)
        end <- sep - 1

        daty <- format(daty, "%Y-%m-%d")
        ret <- lapply(seq_along(start), function(j){
            y <- x[start[j]:end[j]]
            d <- trimws(strsplit(y[1], " ")[[1]][2])
            if(daty != d) return(NULL)

            list(log = paste0(y, collapse = "\n"), 
                 update = any(grepl("Updated", y)))
        })

        inull <- sapply(ret, is.null)
        if(all(inull)) return(convJSON(out))
        ret <- ret[!inull]
        log <- lapply(ret, '[[', 'log')
        update <- lapply(ret, '[[', 'update')

        out$log <- rev(do.call(c, log))
        out$update <- rev(do.call(c, update))
        out$status <- "ok"
    }else{
        coordAWS <- readCoordsAWS(aws_dir)
        rem <- c("SECTOR", "DISTRICT", "start", "end", "timestep", "Type", "PARS")
        coordAWS <- coordAWS[, !names(coordAWS) %in% rem]
        aws_net <- coordAWS$AWSGroup
        aws_list <- coordAWS$id

        dirLOG <- file.path(aws_dir, 'RAW', aws_net, 'LOG', 'AWS', aws_list)
        daty <- as.Date(date, "%Y-%m-%d")
        pattern <- format(daty, "%Y%m%d")
        pattern <- paste0(pattern, ".+\\.txt$")

        logout <- lapply(seq(length(dirLOG)), function(j){
            awslog <- list.files(dirLOG[j], pattern)
            if(length(awslog) == 0) return(NULL)

            retlog <- lapply(awslog, function(fl){
                x <- readLines(file.path(dirLOG[j], fl), skipNul = TRUE)
                sep <- grep('\\*\\*\\*.+', x)
                if(length(sep))
                    x <- x[-sep]
                paste0(x, collapse = "\n")
            })

            return(retlog)
        })

        inull <- sapply(logout, is.null)
        if(all(inull)) return(convJSON(out))

        coordAWS <- coordAWS[!inull, , drop = FALSE]
        logout <- logout[!inull]

        logout <- lapply(seq_along(logout), function(j){
            list(crd = coordAWS[j, ], msg = logout[[j]])
        })

        out$log <- logout
        out$status <- "ok"
    }

    return(convJSON(out))
}
