\name{redisWorker}
\alias{redisWorker}
\title{
redisWorker
}
\description{
Initialize a doRedis worker R process.
}
\usage{
redisWorker(queue, host = "localhost", port = 6379, iter = Inf, timeout = 30, log = stdout())
}
\arguments{
  \item{queue}{
A (character) work queue name, or a list or character vector of queue names.
}
  \item{host}{
The Redis server host name or (character) I. P. address.
}
  \item{port}{
The Redis server port number.
}
  \item{iter}{
The maximum number of jobs to execute before exiting the worker loop (defaults
to infinity).
}
  \item{timeout}{
The worker loop terminates if the work queue is deleted after the
specified timeout interval.
}
  \item{log}{
Log messages to the specified destination (defaults to stdout()).
}
}
\details{
The redisWorker function enrolls the current R session in one or
more doRedis worker pools specified by the work queue names. The worker
loop takes over the R session until either the work queue(s) are deleted.
}
\value{
Nothing is returned but status messages are printed to the log
during operation of the worker loop.
}
\author{
B. W. Lewis <blewis@illposed.net>
}
\note{
All doRedis functions require network access to a Redis server (not
included with the doRedis package).
}

\seealso{
\code{\link{registerDoRedis}}
}
\examples{
\dontrun{
require('doRedis')
redisWorker('jobs')
}
}
