#' Get wind data.
#'
#' Get wind data for wind barb display.
#' 
#' @param tstep time step of the data.
#' @param aws AWS ID.
#' @param start start time.
#' @param end end time.
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

dispWindBarb <- function(tstep, aws, start, end, aws_dir){
    don <- getWindData(tstep, aws, start, end, aws_dir)
    if(is.null(don)){
        ## handle error
        return(NULL)
    }

    time <- 1000 * as.numeric(as.POSIXct(don$wind$date))
    dat <- as.matrix(cbind(time, don$wind[, c('ws', 'wd'), drop = FALSE]))
    dimnames(dat)[[2]] <- NULL

    #########
    if(tstep == "hourly"){
        start <- strptime(start, "%Y-%m-%d-%H", tz = "Africa/Kigali")
        end <- strptime(end, "%Y-%m-%d-%H", tz = "Africa/Kigali")
    }else{
        start <- strptime(start, "%Y-%m-%d-%H-%M", tz = "Africa/Kigali")
        end <- strptime(end, "%Y-%m-%d-%H-%M", tz = "Africa/Kigali")
    }
    start <- format(start, "%Y-%m-%d %H:%M")
    end <- format(end, "%Y-%m-%d %H:%M")

    ttxt <- if(tstep == "hourly") "Hourly" else paste(don$timestep, "minutes")
    stn <- paste(ttxt, "wind data:", don$id, "-", don$stationName)
    perd <- paste("Period:", start, "-", end)
    titre <- paste0(stn, "; ", perd)

    ret <- list(data = dat, title = titre)
    return(convJSON(ret))
}

############
#' Get wind data.
#'
#' Get wind data to display wind rose.
#' 
#' @param tstep time step of the data.
#' @param aws AWS ID.
#' @param start start time.
#' @param end end time.
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return a JSON object
#' 
#' @export

dispWindRose <- function(tstep, aws, start, end, aws_dir){
    ret <- formatFreqTable(tstep, aws, start, end, aws_dir)
    return(convJSON(ret))
}

############
#' Plot wind rose.
#'
#' Plot wind rose for download.
#' 
#' @param tstep time step of the data.
#' @param aws AWS ID.
#' @param start start time.
#' @param end end time.
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return Png image
#' 
#' @export

openairWindrose <- function(tstep, aws, start, end, aws_dir){
    don <- getWindData(tstep, aws, start, end, aws_dir)
    if(is.null(don)){
        ## handle error
        return(NULL)
    }

    breaks <- c(0, 0.5, 2, 4, 6, 8, 12)
    labels <- c(" < 0.5", "0.5 to 2", "2 to 4", "4 to 6", "6 to 8", "8 to 12", " > 12")
    wind <- don$wind
    wind$date <- as.POSIXct(wind$date)

    openair::windRose(wind, angle = 22.5, breaks = breaks,
                      auto.text= FALSE, paddle = FALSE,
                      key = list(labels = labels),
                      key.position = "right") 
}

############
#' Plot wind contour.
#'
#' Plot wind contour.
#' 
#' @param tstep time step of the data.
#' @param aws AWS ID.
#' @param start start time.
#' @param end end time.
#' @param centre center of the image, N, W, S, E.
#' @param aws_dir full path to the directory of the AWS data.\cr
#'               Example: "/home/data/MeteoRwanda_Data/AWS_DATA"
#' 
#' @return CSV object
#' 
#' @export

dispWindContours <- function(tstep, aws, start, end, centre, aws_dir){
    don <- getWindData(tstep, aws, start, end, aws_dir)

    if(tstep == "hourly"){
        start <- strptime(start, "%Y-%m-%d-%H", tz = "Africa/Kigali")
        end <- strptime(end, "%Y-%m-%d-%H", tz = "Africa/Kigali")
    }else{
        start <- strptime(start, "%Y-%m-%d-%H-%M", tz = "Africa/Kigali")
        end <- strptime(end, "%Y-%m-%d-%H-%M", tz = "Africa/Kigali")
    }
    start <- format(start, "%Y-%m-%d %H:%M")
    end <- format(end, "%Y-%m-%d %H:%M")

    if(is.null(don)){
        ## handle error
        # return(NULL)
        txt <- paste("No data for", aws, "\n")
        txt <- paste(txt, "over the period", start, "-", end)

        plot(1, type = 'n', xaxt = 'n', yaxt = 'n', xlab = '', ylab = '', bty = "n")
        text(1, 1, txt, font = 2, cex = 1.5)
    }else{
        hr <- format(don$wind$date, "%H")

        ttxt <- if(tstep == "hourly") "Hourly" else paste(don$timestep, "minutes")
        titre <- paste(ttxt, "wind data:", don$id, "-", don$stationName)
        subtitre <- paste("Period:", start, "-", end, "; Data availability:", paste0(don$avail, "%"))

        windContours(hour = hr,
                     wd = don$wind$wd,
                     ws = don$wind$ws,
                     centre = centre,
                     ncuts = 0.5,
                     spacing = 2,
                     key.spacing = 2,
                     smooth.contours = 1.5,
                     smooth.fill = 1.5,
                     title = list(title = titre, subtitle = subtitre)
                   )
    }
}

###########################

formatFreqTable <- function(tstep, aws, start, end, aws_dir){
    don <- windFrequencyTable(tstep, aws, start, end, aws_dir)
    if(is.null(don)){
        ## handle error
        return(NULL)
    }

    wind <- don$freq
    cnom <- colnames(wind)
    rnom <- rownames(wind)
    scol <- colSums(wind)
    srow <- rowSums(wind)
    wind <- cbind(wind, srow)
    wind <- rbind(wind, c(scol, NA))
    wind <- rbind(wind, c(don$mean, rep(NA, ncol(wind) - 1)))
    wind <- rbind(wind, c(don$calm, rep(NA, ncol(wind) - 1)))
    wind <- rbind(wind, c(don$avail, rep(NA, ncol(wind) - 1)))

    dimnames(wind) <- NULL
    wind <- round(wind, 2)
    wind[18, 1] <- paste(wind[18, 1], "m/s")
    wind[19, 1] <- paste(wind[19, 1], "%")
    wind[20, 1] <- paste(wind[20, 1], "%")
    wind <- cbind(c(rnom, "Total", "Mean Speed", "Percent Calm", "Data Availability"), wind)
    wind[is.na(wind)] <- ''
    wind <- as.data.frame(wind, stringsAsFactors = FALSE)
    names(wind) <- c('Direction', paste(cnom, 'm/s'), 'Total')

    return(as.list(wind))
}

###########################

## adapted the function windContours from https://github.com/tim-salabim/metvurst/
windContours <- function(hour, wd, ws, centre = "S",
                         ncuts = 0.5, spacing = 2, key.spacing = 2,
                         smooth.contours = 1.2, smooth.fill = 1.2,
                         colour = rev(RColorBrewer::brewer.pal(11, "Spectral")),
                         gapcolor = "grey50",
                         title = NULL)
{
    centre <- if(centre == "E") "ES" else centre
    levs <- switch(centre,
                   "N" = c(19:36, 1:18),
                   "ES" = c(28:36, 1:27),
                   "S" = 1:36,
                   "W" = c(10:36, 1:9))

    wdn <- c(45, 90, 135, 180, 225, 270, 315, 360)
    wdc <- c("NE", "E", "SE", "S", "SW", "W", "NW", "N")
    label <- switch(centre,
                    "N" = c(225, 270, 315, 360, 45, 90, 135, 180),
                    "ES" = c(315, 360, 45, 90, 135, 180, 225, 270),
                    "S" = c(45, 90, 135, 180, 225, 270, 315, 360),
                    "W" = c(135, 180, 225, 270, 315, 360, 45, 90))
    label <- wdc[match(label, wdn)]

    hour <- as.numeric(hour)
    dircat <- ordered(ceiling(wd/10), levels = levs, labels = 1:36)
    tab.wd <- stats::xtabs(~ dircat + hour)

    tab.wd_smooth <- fields::image.smooth(tab.wd, theta = smooth.contours, xwidth = 0, ywidth = 0)
    freq.wd <- matrix(prop.table(tab.wd_smooth$z, 2)[, 24:1] * 100, nrow = 36, ncol = 24)

    tab.add_smooth <- fields::image.smooth(tab.wd, theta = smooth.fill, xwidth = 0, ywidth = 0)
    mat.add <- matrix(prop.table(tab.add_smooth$z, 2)[, 24:1] * 100,  nrow = 36, ncol = 24)

    zlevs.fill <- seq(floor(min(mat.add)), ceiling(max(mat.add)), by = ncuts)
    zlevs.conts <- seq(floor(min(freq.wd)), ceiling(max(freq.wd)), by = spacing)

    kolorfun <- grDevices::colorRampPalette(colour)
    kolor <- kolorfun(length(zlevs.fill) - 1)

    mat.add <- rbind(mat.add, mat.add[1, ])
    freq.wd <- rbind(freq.wd, freq.wd[1, ])
    xax <- 1:37
    yax <- 0:23

    ####

    layout.matrix <- matrix(c(0, 1, 1, 1, 1, 0,
                              2, 2, 2, 3, 3, 3,
                              0, 0, 4, 4, 0, 0,
                              0, 0, 5, 5, 0, 0),
                              ncol = 6, nrow = 4, byrow = TRUE)
    layout(layout.matrix, widths = 1, heights = c(0.11, 0.9, 0.07, 0.07), respect = FALSE)

    ####

    op <- par(mar = c(0.1, 1, 1, 1))
    plot(1, type = 'n', xaxt = 'n', yaxt = 'n', xlab = '', ylab = '', bty = "n")
    text(1, 1.1, title$title, font = 2, cex = 2)
    text(1, 0.7, title$subtitle, font = 1, cex = 1.6)
    par(op)

    #### 
    op <- par(mar = c(5.1, 5.1, 0, 0))
    plot(1, xlim = range(xax), ylim = c(-0.5, 23.5), xlab = "", ylab = "",
         type = "n", xaxt = 'n', yaxt = 'n', xaxs = "i", yaxs = "i")
    axis(side = 1, at = seq(4.5, 36 ,by = 4.5), labels = label, font = 2, cex.axis = 1.2)
    axis(side = 2, at = seq(22, 2, -2) - 1, labels = seq(2, 22, 2), las = 1, font = 2, cex.axis = 1.2)
    mtext(side = 1, line = 3, "Direction", font = 2, cex = 0.9)
    mtext(side = 2, line = 3, "Hour", font = 2, cex = 0.9)
    box()

    .filled.contour(xax, yax, mat.add, levels = zlevs.fill, col = kolor)
    .filled.contour(xax, yax, mat.add, levels = seq(0, 0.2, 0.1), col = gapcolor)
    contour(xax, yax, freq.wd, add = TRUE, levels = zlevs.conts, col = "grey10", labcex = 0.7,
            labels = seq(zlevs.fill[1], zlevs.fill[length(zlevs.fill)], key.spacing))
    contour(xax, yax, freq.wd, add = TRUE, levels = 0.5, col = "grey10", lty = 3, labcex = 0.7)
    par(op)

    ####
    op <- par(mar = c(5.1, 0, 0, 1.2))
    plot(1, xlim = range(ws, na.rm = TRUE) + c(-0.1, 0.5), ylim = c(0.5, 24.5), type = 'n',
         xaxt = 'n', yaxt = 'n', xlab = "", ylab = "", xaxs = "i", yaxs = "i")
    abline(h = seq(22, 2, -2), col = "lightgray", lty = "dotted", lwd = 1.0)
    abline(v = axTicks(1), col = "lightgray", lty = "solid", lwd = 0.9)
    axis(side = 1, at = axTicks(1), font = 2, cex.axis = 1.2)
    mtext(side = 1, line = 3, "Speed [m/s]", font = 2, cex = 0.9)

    boxplot(ws ~ rev(hour), horizontal = TRUE, xaxt = 'n', yaxt = 'n', add = TRUE, notch = TRUE,
            col = 'lightblue', medcol = 'red', whiskcol = 'blue', staplecol = 'blue',
            boxcol = 'blue', outcol = 'blue', outbg = 'lightblue', outcex = 0.7, outpch = 21, boxwex = 0.4)
    par(op)

    ####
    op <- par(mar = c(2.0, 1, 0, 1))
    nBreaks<- length(zlevs.fill)
    midpoints<- (zlevs.fill[1:(nBreaks - 1)] +  zlevs.fill[2:nBreaks])/2
    mat.lez <- matrix(midpoints, nrow = 1, ncol = length(midpoints)) 
    image(zlevs.fill, 1:2, t(mat.lez), col = kolor, breaks = zlevs.fill, xaxt = "n", yaxt = "n", xlab = "", ylab = "")
    axis.args <- list(side = 1, mgp = c(3, 1, 0), las = 0, font = 2, cex.axis = 1.5)
    do.call("axis", axis.args)
    box()
    par(op)

    op <- par(mar = c(0, 1, 0, 1))
    plot(1, type = 'n', xaxt = 'n', yaxt = 'n', xlab = '', ylab = '', bty = "n")
    text(1, 1, "Frequencies (in %)", font = 2, cex = 1.5)
    par(op)

    invisible()
}
