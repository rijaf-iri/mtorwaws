
#' Get AWS metadat.
#'
#' Get AWS metadat.
#' 
#' @param aws_dir full path to the directory of the AWS data.
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

aws_metadata <- function(aws_dir){
    aws_group <- c("REMA.csv", "LSI-ELOG.csv", "LSI-XLOG.csv")
    aws_crds <- lapply(aws_group, function(fl){
        aws_fl <- file.path(aws_dir, "COORDS", fl)
        crd <- read.table(aws_fl, sep = ",", header = TRUE,
                          colClasses = "character",
                          stringsAsFactors = FALSE)
        crd
    })
    aws_crds <- do.call(rbind.data.frame, aws_crds)

    infos <- parseInfoAWS(aws_dir)
    aws_infos <- lapply(seq_along(infos), function(j){
        aws <- infos[[j]]
        nom_aws <- c("id", "start", "end", "vars", "tstep", "AWS")
        aws <- aws[nom_aws]
        names(aws) <- c("id", "start", "end", "variables", "timestep", "network")

        crds <- aws_crds[aws_crds$id == aws$id, , drop = FALSE]
        if(nrow(crds) == 0) crds[1, ] <- NA
        crds <- as.list(crds)
        nom_crd <- c('stationName', 'longitude', 'latitude',
                     'elevation', 'SECTOR', 'DISTRICT')
        crds <- crds[nom_crd]
        names(crds) <- c('name', 'longitude', 'latitude',
                         'elevation', 'sector', 'district')
        c(aws, crds)
    })

    aws_net <- do.call(c, lapply(aws_infos, "[[", "network"))
    aws_infos <- split(aws_infos, aws_net)

    aws_infos <- lapply(aws_infos, function(net){
        aws_id <- do.call(c, lapply(net, "[[", "id"))
        names(net) <- aws_id
        net
    })

    return(convJSON(aws_infos))
}
