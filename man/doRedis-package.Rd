\name{doRedis-package}
\alias{doRedis-package}
\alias{doRedis}
\docType{package}
\title{
A Redis-based back end for parallel computing with foreach.
}
\description{
The doRedis package supplies a lightweight, cross-platform 
parallel back end for the foreach \code{\%dopar\%} function.
}
\details{
The foreach package for R defines a modular interface for back end
parallel processing implementations. The doRedis package imlpements a simple
but very flexible parallel back end that uses Redis for inter-process
communication.

The doRedis package requires a connection to an available Redis
server (not included with the package).

Two foreach parameters are specific to the doRedis back end:
\code{chunkSize} (default value 1), and
\code{ftinterval} (default value 30).
The \code{chunkSize} option sets the default number of jobs that
are doled out to each worker process. Jobs are doled out one
at a time by default.
Setting the chunk size larger for shorter-running jobs can
substantially improve performance. Setting this value too high can
negatively impact load-balancing across workers, however.

The \code{ftinterval} option sets the number of seconds between checks
for back end worker failures. Failed jobs will be re-submitted after
this interval.
}
\author{
B. W. Lewis <blewis@illposed.net>
}
\references{
http://cran.r-project.org/web/packages/foreach/index.html
}
\seealso{
\code{\link[foreach]{foreach}}
}
\examples{
\dontrun{
# The example assumes that a Redis server is running on the local host
# and standard port.

# 1. Open one or more 'worker' R sessions and run:
require('doRedis')
redisWorker('jobs')

# We use the name 'jobs' to identify a work queue.
# 2. Open another R session acting as a 'master' and run this simple 
#    sampling approximation of pi:
require('doRedis')
registerDoRedis('jobs')
foreach(j=1:10,.combine=sum,.multicombine=TRUE) \%dopar\% 
          4*sum((runif(1000000)^2 + runif(1000000)^2)<1)/10000000
}
}
