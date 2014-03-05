\name{setTaskLabel}
\alias{setTaskLabel}
\title{
setTaskLabel
}
\description{
Define a function that uniquely labels collections of tasks.
}
\usage{
setTaskLabel(fn = I)
}
\arguments{
  \item{fn}{
A task labeling function, see details below.
}
}
\details{
The \code{doRedis} package organizes work into a collections of tasks called a
job. One job may contain several uniquely identified task collections, and jobs
themselves are uniquely identified. The task collections are labeled by the
\code{taskLabel} function.

The \code{taskLabel} function must take exactly one argument, a single integer
value. Its output must be coerceable into a character string and the function
must be injective.

The default \code{getTask} function removes tasks in the order that they appear
in the hash table. Custom \code{getTask} functions can be defined often in
association with custom \code{taskLabel} functions. The custom functions can
instruct Redis to perferentially dole out tasks based on network distance to
data, for example.
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
# The default getTask function defines a Redis Lua script that returns the first
# listed task or NULL:sk <-  function(queue, job_id, ...)
getTask <- function(queue, job_id, ...)
{
  key <- sprintf("%s:%s",queue, job_id)
  redisEval("local x=redis.call('hkeys',KEYS[1])[1];
             if x==nil then return nil end;
             local ans=redis.call('hget',KEYS[1],x);
             redis.call('hdel',KEYS[1],x);i
             return ans",key)
}

setTaskLabel(fn = I)
}
}
