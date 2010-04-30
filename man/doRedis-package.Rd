\name{doRedis-package}
\alias{doRedis-package}
\alias{doRedis}
\docType{package}
\title{
A Redis-based backend for parallel computing with foreach.
}
\description{
The doRedis package supplies a simple and lightweight 
parallel backend for the foreach \code{\%dopar\%} function.
}
\details{
The foreach package for R defines a modular interface for backend
parallel processing implementations. The doRedis imlpements a simple
but very flexible parallel backend that uses Redis for inter-process
communication.

The doRedis package requires a connection to an available Redis
server (not included with the package).
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
