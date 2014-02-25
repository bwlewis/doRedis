\name{setGetTask}
\alias{setGetTask}
\title{
setGetTask
}
\description{
Define a function used by workers to pull tasks.
}
\usage{
setGetTask(fn = default_getTask)
}
\arguments{
  \item{fn}{
A task pulling function, see details below.
}
}
\details{
The \code{doRedis} package organizes work into a collections of tasks called a
job. One job may contain several uniquely identified task collections, and jobs
themselves are uniquely identified. The task collections are labeled by the
\code{taskLabel} function.

A \code{doRedis} task is specifically a list of two elements: \code{task_id}
and \code{args}, that specify the unique ID of the task collection, and the
\code{foreach} loop expression arguments, respectively.

As of version 1.1.0 of the \code{doRedis} package, task collections are placed
in a Redis \code{hash} table identified by the job ID.  The \code{getTask}
function is used by the R worker processes to pull tasks from this hash table.
The \code{getTask} function must take at least two arguments, \code{queue} and
\code{job_id} that specify the job queue and job ID, respectively.  The
function should work with Redis to obtain and return a task collection, or
return \code{NULL} if no tasks are available.

The \code{getTask} function almost always defines a short Lua scipt run on
the Redis server.

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
setGetTask(getTask)
}
}
