% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/app_plotchart_aws.R
\name{displayTableAgrrAWS}
\alias{displayTableAgrrAWS}
\title{Get aggregated data.}
\usage{
displayTableAgrrAWS(tstep, aws, start, end, aws_net, aws_dir)
}
\arguments{
\item{tstep}{time step.}

\item{aws}{AWS ID.}

\item{start}{start date.}

\item{end}{end date.}

\item{aws_net}{AWS network.}

\item{aws_dir}{full path to the directory of the AWS data.\cr
Example: "/home/data/MeteoRwanda_Data/AWS_DATA"}
}
\value{
a JSON object
}
\description{
Get aggregated data to display on table.
}
