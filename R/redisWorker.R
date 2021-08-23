# Do not use .setOK from interactive R sessions.
# .setOK and .delOK support worker fault tolerance
`.setOK` <- function(port, host, key, password, timelimit=0)
{
  if(missing(password)) password <- ""
  if(is.null(password)) password <- ""
  invisible(
    .Call(C_setOK, as.integer(port), as.character(host),
        as.character(key), as.character(password), as.double(timelimit), PACKAGE="doRedis"))
}

`.delOK` <- function()
{
  invisible(.Call(C_delOK, PACKAGE="doRedis"))
}

# .workerInit runs once per worker when it encounters a new job ID
# expr, exportenv, parentenv, packages are parameters from
# the job environment, see invocation of .workerInit below.
# If an error is encountered at any step here, the expression to be
# evaluated is replaced with the error for return to the coordinator process.
`.workerInit` <- function(expr, exportenv, parentenv, packages)
{
  assign("expr", expr, .doRedisGlobals)
  assign("exportenv", exportenv, .doRedisGlobals)
  err = tryCatch(
    {
# First load packages
      for (p in packages) library(p, character.only=TRUE)
# Check for .RNGkind and set if possible
      if(!is.null(exportenv[[".RNGkind"]]))
      {
        do.call("RNGkind", list(exportenv[[".RNGkind"]]), envir=globalenv())
      }
# Check for worker.init function
      if(!is.null(exportenv[["worker.init"]]))
        if(is.function(exportenv[["worker.init"]]))
          do.call(exportenv[["worker.init"]], list(), envir=globalenv())
      parent.env(.doRedisGlobals$exportenv) <- 
        if(is.null(parentenv)) globalenv() else getNamespace(parentenv[[1]])
    },
    error=function(e) {
      message(gettext(e))
      e
    }
  )
  if(inherits(err, "error")) assign("expr", err, .doRedisGlobals)
}

# .evalWrapper runs once per worker loop iteration
`.evalWrapper` <- function(args)
{
  tryCatch({
      lapply(names(args), function(n)
                         assign(n, args[[n]], pos=.doRedisGlobals$exportenv))
      if(exists(".Random.seed", envir=.doRedisGlobals$exportenv))
      {
        if(isTRUE(is.integer(.doRedisGlobals$exportenv[[".Random.seed"]])) &&
           isTRUE(length(.doRedisGlobals$exportenv[[".Random.seed"]]) == 1)) {
           set.seed(.doRedisGlobals$exportenv[[".Random.seed"]])
        }
        rm(list=".Random.seed", pos=.doRedisGlobals[["exportenv"]])
      }
      tryCatch(
      {
# Override the function set.seed.worker to roll your own RNG.
        if(exists("set.seed.worker", envir=.doRedisGlobals$exportenv))
          do.call("set.seed.worker", list(0), envir=.doRedisGlobals$exportenv)
       }, error=function(e) cat(as.character(e), "\n"))
      eval(.doRedisGlobals[["expr"]], envir=.doRedisGlobals[["exportenv"]])
    },
    error=function(e) e
  )
}

#' Start one or more background R worker processes on the local system.
#'
#' Use \code{startLocalWorkers} to start one or more doRedis R worker processes
#' in the background. The worker processes are started on the local system using
#' the \code{redisWorker} function.
#'
#' Running workers self-terminate after a \code{linger} period if their work queues are deleted with the
#' \code{removeQueue} function, or when network activity with Redis remains
#' inactive for longer than the \code{timeout} period set in the \code{redisConnect}
#' function. That value defaults internally to 3600 (one hour) in \code{startLocalWorkers}.
#' You can increase it by including a {timeout=n} argument value.
#'
#' @param n number of workers to start
#' @param queue work queue name
#' @param host Redis database host name or IP address
#' @param port Redis database port number
#' @param iter maximum number of tasks to process before exiting the worker loop
#' @param linger timeout in seconds after which the work queue is deleted that the worker terminates
#' @param log print messages to the specified file connection
#' @param Rbin full path to the command-line R program
#' @param password optional Redis database password
#' @param ... optional additional parameters passed to the \code{\link{redisWorker}} function
#'
#' @return NULL is invisibly returned.
#'
#' @seealso \code{\link{registerDoRedis}}, \code{\link{redisWorker}}
#'
#' @examples
#' # Only run if a Redis server is running
#' if (redux::redis_available()) {
#' ## The example assumes that a Redis server is running on the local host
#' ## and standard port.
#'
#' # Start a single local R worker process
#' startLocalWorkers(n=1, queue="R jobs", linger=1)
#'
#' # Run a simple sampling approximation of pi:
#' registerDoRedis("R jobs")
#' print(foreach(j=1:10, .combine=sum, .multicombine=TRUE) %dopar%
#'         4 * sum((runif(1000000) ^ 2 + runif(1000000) ^ 2) < 1) / 10000000)
#'
#' # Clean up
#' removeQueue("R jobs")
#' }
#'
#' @export
startLocalWorkers <- function(n, queue, host="localhost", port=6379,
  iter=Inf, linger=30, log=stdout(),
  Rbin=paste(R.home(component="bin"),"R",sep="/"), password, ...)
{
  m <- match.call()
  f <- formals()
  l <- m[["log"]]
  if(is.null(l)) l <- f$log
  conargs <- list(...)
  if(is.null(conargs$timeout)) conargs$timeout <- 3600
  conargs <- paste(paste(names(conargs), conargs, sep="="), collapse=",")

  # ensure that we pass multiple queues, if applicable, to each worker
  queue <- sprintf("c(%s)", paste("'", queue, "'", collapse=", ", sep=""))

  cmd <- paste("require(doRedis);redisWorker(queue=",
      queue, ", host='", host,"', port=", port,", iter=", iter,", linger=",
      linger, ", log=", deparse(l), sep="")
  if(nchar(conargs) > 0) cm <- sprintf("%s, %s", cmd, conargs)
  if(!missing(password)) cmd <- sprintf("%s, password='%s'", cmd, password)
  dots <- list(...)
  if(length(dots) > 0)
  {
    dots <- paste(paste(names(dots), dots, sep="="), collapse=",")
    cmd <- sprintf("%s,%s", cmd, dots)
  }
  cmd <- sprintf("%s)", cmd)
  cmd <- gsub("\"", "'", cmd)

  j <- 0
  args <- c("--slave", "-e", paste("\"", cmd,"\"", sep=""))
  while(j < n)
  {
    system(paste(c(Rbin, args), collapse=" "), intern=FALSE, wait=FALSE)
    j <- j + 1
  }
}

#' Initialize a doRedis worker process.
#'
#' The redisWorker function enrolls the current R session in one or
#' more doRedis worker pools specified by the work queue names. The worker
#' loop takes over the R session until the work queue(s) are deleted, after
#' which the worker loop exits after the \code{linger} period, or until
#' the worker has processed \code{iter} tasks.
#' Running workers also terminate after network activity with Redis remains
#' inactive for longer than the \code{timeout} period set in the \code{redisConnect}
#' function. That value defaults internally to 30 seconds in \code{redisWorker}.
#' You can increase it by including a {timeout=n} argument value.
#'
#'
#' @param queue work queue name or a vector of queue names
#' @param host Redis database host name or IP address
#' @param port Redis database port number
#' @param iter maximum number of tasks to process before exiting the worker loop
#' @param linger timeout in seconds after which the work queue is deleted that the worker terminates
#' @param log print messages to the specified file connection
#' @param connected set to \code{TRUE} to reuse an existing open connection to Redis, otherwise establish a new one
#' @param password optional Redis database password
#' @param loglevel set to > 0 to increase verbosity in the log
#' @param timelimit set to > 0 to specify a task time limit in seconds, after which worker processes are killed; beware that setting this value > 0 will terminate any R worker process if their task takes too long.
#' @param ... Optional additional parameters passed to \code{\link{redisConnect}}
#' @note The worker connection to Redis uses a TCP timeout value of 30 seconds by
#' default. That means that the worker will exit after about 30 seconds of inactivity.
#' If you want the worker to remain active for longer periods, set the \code{timeout}
#' option to a larger value.
#'
#' Use the \code{linger} option to instruct the worker to linger for up to the indicated
#' number of seconds after the listening work queue has been removed. After at most that
#' interval, the worker will exit after removing the queue.
#'
#' @return NULL is invisibly returned.
#'
#' @seealso \code{\link{registerDoRedis}}, \code{\link{startLocalWorkers}}
#' @importFrom utils capture.output head tail object.size
#'
#' @export
redisWorker <- function(queue, host="localhost", port=6379,
                        iter=Inf, linger=30, log=stderr(),
                        connected=FALSE, password=NULL, loglevel=0, timelimit=0, ...)
{
  if (!connected)
  {
    conargs <- list(...)
# Set low default connection timeout, see issue #34
    if(is.null(conargs$timeout)) conargs$timeout <- 30
    conargs <- c(host=host, port=port, password=password, conargs)
    do.call("redisConnect", args=conargs)
  }
  if(is.character(log))
    log <- file(log, open="w+")
  sink(type="message", file=log)
  assign(".jobID", "0", envir=.doRedisGlobals)

  queueLive <- paste(queue, "live", sep=".")
  for(j in queueLive)
    if(!redisExists(j)) redisSet(j, "")

  queueCount <- paste(queue,"count",sep=".")
  for (j in queueCount)
    tryCatch(redisIncr(j),error=function(e) invisible())

  if(interactive()) cat("Waiting for doRedis jobs.\n")
  k <- 0
  on.exit(.delOK()) # In case we exit this function unexpectedly
  while(k < iter)
  {
    work <- tryCatch(redisBLPop(queue, timeout=linger), error=function(e) list())
    if(isTRUE(!is.null(globalenv()[[".redis.debug"]]) && globalenv()[[".redis.debug"]] > 0))
      cat(paste(capture.output(print(work)), collapse="\n"), "\n", file=stderr())
# Note the apparent fragility here. The worker has downloaded a task but
# not yet set alive/started keys. If a failure occurs before that, it
# seems like the task has been consumed and finished but no matching result
# appears in the output queue. But, the coordinator keeps track of missing output
# and will eventually re-submit such lost tasks.
    if(length(work) == 0) work <- NULL
    myQueue <- head(names(work), 1) # note that the worker might listen on multiple queues
    queueEnv <- paste(myQueue, "env", work[[1]]$ID, sep=".")
    queueOut <- paste(myQueue, "out", work[[1]]$ID, sep=".")
# We terminate the worker loop after a timeout when all specified work
# queues have been deleted.
    if(is.null(work[[1]]))
     {
      ok <- FALSE
      for (j in queueLive) ok <- ok || redisExists(j)
      if(!ok) {
# If we get here, ALL our queues were deleted. Clean up and exit worker loop.
        for (j in queueOut) if(redisExists(j)) redisDelete(j)
        for (j in queueEnv) if(redisExists(j)) redisDelete(j)
        for (j in queueCount) if(redisExists(j)) redisDelete(j)
        for (j in queue) if(redisExists(j)) redisDelete(j)
        break
      }
     }
    else
     {
# FT support
      iters <- names(work[[1]]$argsList)
      fttag <- sprintf("iters %s...%s host %s pid %s begin %s", iters[1],
                 iters[length(iters)], Sys.info()["nodename"], Sys.getpid(), gsub(" ", "-", Sys.time()))
      fttag.start <- paste(myQueue, "start", work[[1]]$ID, fttag, sep=".")
      fttag.alive <- paste(myQueue, "alive", work[[1]]$ID, fttag, sep=".")
# fttag.start is a permanent key
# fttag.alive is a matching ephemeral key that is regularly kept alive by the
# setOK helper thread. Upon disruption of the thread (for example, a crash),
# the resulting Redis state will be an unmatched start tag, which may be used
# by fault tolerant code to resubmit the associated jobs.
      redisSet(fttag.alive, 0)
      redisExpire(fttag.alive, 10)
      .setOK(port, host, fttag.alive, password=password, timelimit=timelimit) # refresh thread
      redisSet(fttag.start, as.integer(names(work[[1]]$argsList))) # then set a started key
# Now do the work.
      k <- k + 1
      if(loglevel > 0)
      {
        cat("Processing task(s)",
         paste(head(names(work[[1]]$argsList), 1),
          tail(names(work[[1]]$argsList), 1), sep="...", collapse="..."),
           "from queue", names(work), "ID", work[[1]]$ID, "\n", file=log)
      }
# Check that the incoming work ID matches our current environment. If
# not, we need to re-initialize our work environment with data from the
# <queue>.env Redis string.
      if(get(".jobID", envir=.doRedisGlobals) != work[[1]]$ID)
       {
        initdata <- redisGet(queueEnv)
        .workerInit(initdata$expr, initdata$exportenv, initdata$parentenv, initdata$packages)
        assign(".jobID", work[[1]]$ID, envir=.doRedisGlobals)
       }
      result <- lapply(work[[1]]$argsList, .evalWrapper)
      names(result) <- names(work[[1]]$argsList)
# We saw that long-running jobs can sometimes lose connections to
# Redis in an AWS EC2 example. The following tries to re-establish
# a redis connecion on error here.
      tryCatch( redisLPush(queueOut, result), error=function(e)
      {
        cat(as.character(e), file=log)
        tryCatch(redisClose(), error=function(e) invisible())
        do.call("redisConnect", args=conargs)
        redisLPush(queueOut, result)
      })
# Importantly, the worker does not delete his start key until after the
# result is successfully placed in a Redis queue. And then after that
# the alive thread is terminated, allowing the corresponding alive key
# to expire.
      tryCatch(redisDelete(fttag.start), error=function(e) invisible())
      .delOK()
    }
  }
# Either the queue has been deleted, or we've exceeded the number of
# specified work iterations.
  for (j in queueCount) if(redisExists(j)) redisDecr(j)
  cat("Normal worker exit.\n", file=log)
  if (!connected) redisClose()
}
