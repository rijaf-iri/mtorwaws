% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/app_plotmap_aws.R
\name{displayMAPAggr}
\alias{displayMAPAggr}
\title{Get AWS aggregated spatial data.}
\usage{
displayMAPAggr(tstep, time, aws_dir)
}
\arguments{
\item{tstep}{the time step of the data.}

\item{time}{the time to display in the format,
hourly: "YYYY-MM-DD-HH",
daily: "YYYY-MM-DD",
pentad: "YYYY-MM-DD",
dekadal: "YYYY-MM-DD",
monthly: "YYYY-MM"}

\item{aws_dir}{full path to the directory of the AWS data.\cr
Example: "/home/data/MeteoRwanda_Data/AWS_DATA"}
}
\value{
a JSON object
}
\description{
Get AWS aggregated spatial data to display on map.
}
