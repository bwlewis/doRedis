% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/doRedis.R
\name{setChunkSize}
\alias{setChunkSize}
\title{Set the default granularity of distributed tasks.}
\usage{
setChunkSize(value = 1)
}
\arguments{
\item{value}{positive integer chunk size setting}
}
\value{
\code{value} is invisibly returned.
}
\description{
A job is the collection of all tasks in a foreach loop.
A task is a collection of loop iterations of at most size \code{chunkSize}.
R workers are assigned work by task in blocks of at most
\code{chunkSize} loop iterations per task.
The default value is one iteration per task.
Setting the default chunk size larger for shorter-running jobs can
substantially improve performance. Setting this value too high can
negatively impact load-balancing across workers, however.
}
\examples{
\dontrun{
setChunkSize(5)
foreach(j=1:10) \%dopar\% j
}

}
