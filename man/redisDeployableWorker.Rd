\name{redisDeployableWorker}
\alias{redisDeployableWorker}
\title{
redisDeployableWorker
}
\description{
Initialize a doRedis deployable worker R process.
}
\usage{
redisDeployableWorker(resourceQueue, host = "localhost", port = 6379, iter = Inf, timeout = 30, log = stdout())
}
\arguments{
  \item{resourceQueue}{
A (character) resource queue name.
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
The redisDeployableWorker function enrolls the current R session in a 
resource worker pool specified by the resource queue name. The worker waits
for the location of work queue and, upon receiving it, deploys to the
work queue, performing tasks until either the work queue is deleted or the 
worker times out waiting for a new task. After this the worker returns to 
waiting on the resource queue for subsequent deployment.
}
\value{
Nothing is returned but status messages are printed to the log
during operation of the worker loop.
}
\author{
Michael Kane <kaneplusplus@gmail.com>
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
redisDeployableWorker('resources')
}
}
