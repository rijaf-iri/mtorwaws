
parseInfoAWS <- function(dirAWS){
    awsNET <- c("REMA", "LSI-ELOG", "LSI-XLOG")
    infos <- lapply(awsNET, function(net){
        dirINFO <- file.path(dirAWS, "RAW", net, "INFO")
        awsInfo <- list.files(dirINFO, "\\.rds$")
        info <- lapply(awsInfo, function(x){
            aws <- readRDS(file.path(dirINFO, x))
            aws$AWS <- net
            if(net == "REMA"){
                wnd <- c('FF', 'FFmax')
                if(all(wnd %in% names(aws$vars))){
                    aws$vars$FF <- c("Ave", "Max")
                    aws$vars <- aws$vars[!names(aws$vars) %in% 'FFmax']
                }
                if('FFmax' %in% names(aws$vars)){
                    nom <- names(aws$vars)
                    nom[nom == "FFmax"] <- "FF"
                    names(aws$vars) <- nom
                }
            }

            return(aws)
        })
        info
    })
    infos <- do.call(c, infos)
    return(infos)
}

readCoordsAWS <- function(dirAWS){
    aws.group <- c("REMA.csv", "LSI-ELOG.csv", "LSI-XLOG.csv")
    aws.crds <- lapply(aws.group, function(fl){
        aws.fl <- file.path(dirAWS, "COORDS", fl)
        crd <- read.table(aws.fl, sep = ",", header = TRUE,
                          colClasses = "character",
                          stringsAsFactors = FALSE)
        crd
    })
    aws.crds <- do.call(rbind.data.frame, aws.crds)

    infos <- parseInfoAWS(dirAWS)
    aws.id <- do.call(c, lapply(infos, "[[", "id"))

    iaws <- aws.id %in% aws.crds$id
    infos <- infos[iaws]
    aws.id <- aws.id[iaws]

    aws.name <- sapply(lapply(infos, "[[", "name"),
                       function(x) if(is.null(x)) NA else x)
    start <- do.call(c, lapply(infos, "[[", "start"))
    end <- do.call(c, lapply(infos, "[[", "end"))

    tstep <- lapply(infos, "[[", "tstep")
    tstep[sapply(tstep, length) == 0] <- NA
    tstep <- do.call(c, tstep)

    awsGRP <- do.call(c, lapply(infos, "[[", "AWS"))
    pars <- lapply(lapply(infos, "[[", "vars"), names)

    ix <- match(aws.id, aws.crds$id)
    crds <- aws.crds[ix, ]
    crds$id <- aws.id
    crds$AWSGroup <- awsGRP

    inm <- !is.na(aws.name) & is.na(crds$stationName)
    crds$stationName[inm] <- aws.name[inm]
    crds <- cbind(crds, start = start, end = end, timestep = tstep)
    crds$PARS <- pars
    rownames(crds) <- NULL

    return(crds)
}
