\name{startLocalWorkers}
\alias{startLocalWorkers}
\title{
startLocalWorkers
}
\description{
Start background R worker processes on the local system.
}
\usage{
startLocalWorkers(n, queue, host = "localhost", port = 6379, iter = Inf, timeout = 60, log = stderr(),  Rbin = paste(R.home(component='bin'),"/R --slave",sep=""))
}
\arguments{
  \item{n}{
The number of workers to start.
}
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
Log messages to the specified destination (defaults to stderr()).
}
  \item{Rbin}{
The full path to the command-line R program.
}
}
\details{
Use \code{startLocalWorkers} to start one or more doRedis R worker processes
in the background. The worker processes are started on the local system using
the \code{redisWorker} function.

Running workers self-terminate when their work queues are deleted with the
\code{removeQueue} function.
}
\value{
Nothing is returned. Set the \code{log} parameter to \code{stdout()} to
see log messages printed on standard output of the invoking R session.
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
\code{\link{redisWorker}}
}
\examples{
\dontrun{
require('doRedis')
registerDoRedis('jobs')
startLocalWorkers(n=2, queue='jobs')
print(getDoParWorkers())
foreach(j=1:10,.combine=sum,.multicombine=TRUE) \%dopar\%
          4*sum((runif(1000000)^2 + runif(1000000)^2)<1)/10000000
removeQueue('jobs')
}
}
