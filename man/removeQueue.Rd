\name{removeQueue}
\alias{removeQueue}
\title{
Remove doRedis work queues.
}
\description{
Use the removeQueue function to delete one or more doRedis work queues.
}
\usage{
removeQueue(queue)
}
\arguments{
  \item{queue}{
A character work queue name or vector or list of queue names.
}
}
\details{
This action will terminate the worker loops running on any corresponding
back-end workers. Upon termination the workers will clean up any ancillary
Redis keys.
}
\value{
TRUE is returned if the queues were successfully deleted, FALSE and
probably an error condition otherwise.
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
removeQueue('jobs')
}
}
