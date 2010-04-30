\name{registerDoRedis}
\alias{registerDoRedis}
\title{
Register the doRedis parallel back end for foreach.
}
\description{
The doRedis package supplies a simple and lightweight
parallel backend for the foreach \code{\%dopar\%} function.
}
\usage{
registerDoRedis(queue, host = "localhost", port = 6379)
}
\arguments{
  \item{queue}{
A (character) work queue name.
}
  \item{host}{
The Redis server host or (character) I. P. address.
}
  \item{port}{
The Redis server port number.
}
}
\details{
The doRedis package imlpements a simple but flexible parallel backend 
for foreach that uses Redis for inter-process communication. The work 
queue name specifies the base name of a small set of Redis keys that the master
and worker processes use to exchange data.

Back-end worker R processes  advertise their availablility for work
with the \code{\link{redisWorker}} function.

The doRedis parallel  backend supports dynamic pools of back-end workers. 
New workers may be added
to work queues at any time and can be immediately used by in-flight
foreach computations.
}
\value{
Nothing is returned.
}
\references{
http://cran.r-project.org/web/packages/foreach/index.html
}
\seealso{
\code{\link{redisWorker}}
\code{\link{removeQueue}}
}
\author{
B. W. Lewis <blewis@illposed.net>
}
\note{
All doRedis functions require network access to a Redis server (not
included with the doRedis package).
}
\examples{
\dontrun{
## The example assumes that a Redis server is running on the local host
## and standard port.

## 1. Open one or more 'worker' R sessions and run:
require('doRedis')
redisWorker('jobs')

## 2. Open another R session acting as a 'master' and run this simple 
##    sampling approximation of pi:
require('doRedis')
registerDoRedis('jobs')
foreach(j=1:10,.combine=sum,.multicombine=TRUE) \%dopar\% 
          4*sum((runif(1000000)^2 + runif(1000000)^2)<1)/10000000
removeQueue('jobs')
}
}
