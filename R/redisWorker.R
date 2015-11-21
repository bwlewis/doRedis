# Do not use .setOK from interactive R sessions.
# .setOK and .delOK support worker fault tolerance
`.setOK` <- function(port, host, key, password)
{
  if(missing(password)) password <- ""
  if(is.null(password)) password <- ""
  invisible(
    .Call("setOK", as.integer(port), as.character(host),
        as.character(key),as.character(password), PACKAGE="doRedis"))
}

`.delOK` <- function()
{
  invisible(.Call("delOK",PACKAGE="doRedis"))
}

# .workerInit runs once per worker when it encounters a new job ID
`.workerInit` <- function(expr, exportenv, packages, seed, log)
{
  tryCatch(
    {
      for (p in packages) library(p, character.only=TRUE)
      RNGkind("L'Ecuyer-CMRG")
# Check for worker.init function
      if(!is.null(exportenv$worker.init))
        if(is.function(exportenv$worker.init))
          do.call(exportenv$worker.init, list(), envir=globalenv())
    }, error=function(e) cat(as.character(e), "\n", file=log)
  )
  assign("expr", expr, .doRedisGlobals)
  assign("exportenv", exportenv, .doRedisGlobals)
# XXX This use of parent.env should be changed. It's used here to
# set up a valid search path above the working evironment, but its use
# is fraglie as this may function be dropped in a future release of R.
  parent.env(.doRedisGlobals$exportenv) <- globalenv()
}

`.evalWrapper` <- function(args)
{
  tryCatch({
      lapply(names(args), function(n)
                         assign(n, args[[n]], pos=.doRedisGlobals$exportenv))
      if(exists(".Random.seed",envir=.doRedisGlobals$exportenv))
      {
        assign(".Random.seed",.doRedisGlobals$exportenv$.Random.seed, envir=globalenv())
      }
      tryCatch(
      {
# Override the function set.seed.worker to roll your own RNG.
        if(exists("set.seed.worker",envir=.doRedisGlobals$exportenv))
          do.call("set.seed.worker",list(0),envir=.doRedisGlobals$exportenv)
       }, error=function(e) cat(as.character(e),"\n",file=log))
      eval(.doRedisGlobals$expr, envir=.doRedisGlobals$exportenv)
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
#' Running workers self-terminate when their work queues are deleted with the
#' \code{removeQueue} function.
#'
#' @param n The number of workers to start.
#' @param queue The doRedis work queue name.
#' @param host The Redis database host name or IP address.
#' @param port The Redis database port number.
#' @param iter Maximum number of tasks to process before exiting the worker loop.
#' @param timeout Timeout in seconds after which the work queue is deleted that the worker terminates.
#' @param log Log messages to the specified file connection.
#' @param Rbin The full path to the command-line R program.
#' @param password Optional Redis database password.
#' @param ... Optional additional parameters passed to the \code{\link{redisWorker}} function.
#'
#' @return NULL is invisibly returned.
#'
#' @seealso \code{\link{registerDoRedis}}, \code{\link{redisWorker}}
#'
#' @examples
#' \dontrun{
#' require('doRedis')
#' registerDoRedis('jobs')
#' startLocalWorkers(n=2, queue='jobs')
#' print(getDoParWorkers())
#' foreach(j=1:10,.combine=sum,.multicombine=TRUE) \%dopar\%
#'           4*sum((runif(1000000)^2 + runif(1000000)^2)<1)/10000000
#' removeQueue('jobs')
#' }
#'
#' @export
startLocalWorkers <- function(n, queue, host="localhost", port=6379,
  iter=Inf, timeout=30, log=stdout(),
  Rbin=paste(R.home(component="bin"),"R",sep="/"), password, ...)
{
  m <- match.call()
  f <- formals()
  l <- m$log
  if(is.null(l)) l <- f$log
  cmd <- paste("require(doRedis);redisWorker(queue='",
      queue, "', host='", host,"', port=", port,", iter=", iter,", timeout=",
      timeout, ", log=", deparse(l), sep="")
  if(!missing(password)) cmd <- sprintf("%s,password='%s'", cmd, password)
  dots <- list(...)
  if(length(dots) > 0)
  {
    dots <- paste(paste(names(dots),dots,sep="="),collapse=",")
    cmd <- sprintf("%s,%s",cmd,dots)
  }
  cmd <- sprintf("%s)",cmd)

  j <- 0
  args <- c("--slave","-e",paste("\"",cmd,"\"",sep=""))
  while(j < n)
  {
    system(paste(c(Rbin,args),collapse=" "),intern=FALSE,wait=FALSE)
    j <- j + 1
  }
}

#' Initialize a doRedis worker process.
#'
#' The redisWorker function enrolls the current R session in one or
#' more doRedis worker pools specified by the work queue names. The worker
#' loop takes over the R session until the work queue(s) are deleted, after
#' which at most \code{timeout} seconds the worker loop exits, or until
#' the worker has processed \code{iter} tasks.
#'
#' @param queue The doRedis work queue name or a vector of queue names.
#' @param host The Redis database host name or IP address.
#' @param port The Redis database port number.
#' @param iter Maximum number of tasks to process before exiting the worker loop.
#' @param timeout Timeout in seconds after which the work queue is deleted that the worker terminates.
#' @param log Log messages to the specified file connection.
#' @param connected Is the R session creating the worker already connected to Redis?
#' @param password Optional Redis database password.
#' @param ... Optional additional parameters passed to \code{\link{redisConnect}}
#'
#' @return NULL is invisibly returned.
#'
#' @seealso \code{\link{registerDoRedis}}, \code{\link{startLocalWorkers}}
#'
#' @export
redisWorker <- function(queue, host="localhost", port=6379,
                        iter=Inf, timeout=30, log=stdout(),
                        connected=FALSE, password=NULL, ...)
{
  if (!connected)
    redisConnect(host, port, password=password, ...)
  sink(type="message", append=TRUE, file=log)
  sink(type="output", append=TRUE, file=log)
  assign(".jobID", "0", envir=.doRedisGlobals)
  queueLive <- paste(queue, "live", sep=".")
  if(!redisExists(queueLive)) redisSet(queueLive, "")
  queueCount <- paste(queue,"count",sep=".")
  for (j in queueCount)
    tryCatch(redisIncr(j),error=function(e) invisible())
  cat("Waiting for doRedis jobs.\n", file=log)
  flush.console()
  k <- 0
  on.exit(.delOK()) # In case we exit this function unexpectedly
  while(k < iter)
  {
    work <- redisBLPop(queue, timeout=timeout)
# XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX
# ---------------------------------------------------------------------------
# From this point to the point similarly marked below, things are fragile.
# The worker has downloaded a task but not yet set a started key. If a failure
# occurs here, then the master can't detect it. XXX FIX ME!!!!!
    queueEnv <- paste(queue,"env", work[[1]]$ID, sep=".")
    queueOut <- paste(queue,"out", work[[1]]$ID, sep=".")
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
      fttag <- sprintf("iters %s...%s host %s pid %s", iters[1], iters[length(iters)], Sys.info()["nodename"], Sys.getpid())
      fttag.start <- paste(queue,"start",work[[1]]$ID,fttag,sep=".")
      fttag.alive <- paste(queue,"alive",work[[1]]$ID,fttag,sep=".")
# fttag.start is a permanent key
# fttag.alive is a matching ephemeral key that is regularly kept alive by the
# setOK helper thread. Upon disruption of the thread (for example, a crash),
# the resulting Redis state will be an unmatched start tag, which may be used
# by fault tolerant code to resubmit the associated jobs.
      .setOK(port, host, fttag.alive, password=password) # Immediately set an alive key for this task
      redisSet(fttag.start,as.integer(names(work[[1]]$argsList))) # then set a started key
# ---------------------------------------------------------------------------
# XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX
# End of bad code section (see above). XXX FIX ME!!!
# ---------------------------------------------------------------------------
# Now do the work.
      k <- k + 1
      cat("Processing task(s)", names(work[[1]]$argsList), "from queue", names(work), "ID", work[[1]]$ID, "\n", file=log)
      flush.console()
# Check that the incoming work ID matches our current environment. If
# not, we need to re-initialize our work environment with data from the
# <queue>.env Redis string.
      if(get(".jobID", envir=.doRedisGlobals) != work[[1]]$ID)
       {
        initdata <- redisGet(queueEnv)
        .workerInit(initdata$expr, initdata$exportenv, initdata$packages,
                    names(work[[1]]$argsList)[[1]],log)
        assign(".jobID", work[[1]]$ID, envir=.doRedisGlobals)
       }
      result <- lapply(work[[1]]$argsList, .evalWrapper)
      names(result) <- names(work[[1]]$argsList)
      redisLPush(queueOut, result)
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
  cat("Worker exit.\n", file=log)
  if (!connected) redisClose()
}
