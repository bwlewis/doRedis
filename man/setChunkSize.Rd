\name{setChunkSize}
\alias{setChunkSize}
\title{
setChunkSize
}
\description{
Set the default granularity of distributed tasks.
}
\usage{
setChunkSize(value = 1)
}
\arguments{
  \item{value}{
The new default chunk size.
}
}
\details{
The setChunkSize function lets users set the default number of jobs that
are doled out to each worker process.
The doRedis package doles out jobs one at a time by default. 
Setting the default chunk size larger for shorter-running jobs can
substantially improve performance. Setting this value too high can 
negatively impact load-balancing across workers, however.

This value is overriden by setting the 'chunkSize' option in the
foreach loop (see the examples).
}
\value{
Nothing is returned.
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
require('doRedis')
setChunkSize(10)

## Override the default value in a loop as shown in the following example:
foreach(j=1:1000, .options.redis=list(chunkSize=100)) %dopar% j
}
}
