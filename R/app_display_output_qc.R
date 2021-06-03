
#' Get QC output for minutes data.
#'
#' Get QC output for minutes data.
#' 
#' @param time time range in hour to parse.
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return JSON object
#' 
#' @export

displayQCMinutes <- function(time, aws_dir){
    coordAWS <- readCoordsAWS(aws_dir)
    rem <- c("SECTOR", "DISTRICT", "start", "end", "timestep", "Type", "PARS")
    coordAWS <- coordAWS[, !names(coordAWS) %in% rem]
    aws_net <- coordAWS$AWSGroup
    aws_list <- coordAWS$id

    dirQC <- file.path(aws_dir, 'PROC', 'QCOUT', 'QC1', aws_net, aws_list)
    daty <- strptime(time, "%Y-%m-%d-%H", tz = "Africa/Kigali")
    pattern <- format(daty, "%Y%m%d%H")
    pattern <- paste0(pattern, ".+\\.rds$")

    qcout <- lapply(seq(length(dirQC)), function(j){
        awsqc <- list.files(dirQC[j], pattern)
        if(length(awsqc) == 0) return(NULL)

        retqc <- lapply(awsqc, function(fl){
            x <- readRDS(file.path(dirQC[j], fl))
            nom <- lapply(names(x$qc), function(n) paste0(n, '-', names(x$qc[[n]])))
            qc <- do.call(cbind.data.frame, x$qc)
            names(qc) <- do.call(c, nom)
            list(date = x$date, qc = qc)
        })

        timeqc <- do.call(c, lapply(retqc, '[[', 'date'))
        retqc <- lapply(retqc, '[[', 'qc')
        names(retqc) <- timeqc

        return(retqc)
    })

    inull <- sapply(qcout, is.null)

    ret <- list(status = "no-data", data = NULL)
    if(all(inull)) return(convJSON(ret))

    coordAWS <- coordAWS[!inull, , drop = FALSE]
    qcout <- qcout[!inull]
    
    qcout <- lapply(seq_along(qcout), function(j){
        list(crd = coordAWS[j, ], qc = qcout[[j]])
    })

    ret <- list(status = "ok", data = qcout)

    return(convJSON(ret))
}
