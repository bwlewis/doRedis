<<<<<<< HEAD
# Exposed function to set a variable named "tag" in .doRedisGlobals.
setTag <- function(label)
{
  assign("tag",as.character(label),envir=.doRedisGlobals)
}

# Retrieve a variable "tag" from .doRedisGlobals, if it exists. If not,
# return the system name.
.getTag <- function()
{
  if(exists("tag",envir=.doRedisGlobals)) return(get("tag",envir=.doRedisGlobals))
  Sys.info()["nodename"][[1]]
}

# .setOK and .delOK support worker fault tolerance (internal functions)
.setOK <- function(port, host, key)
=======
# .setOK and .delOK support worker fault tolerance
`.setOK` <- function(port, host, key)
>>>>>>> devel
{
  .Call("setOK", as.integer(port), as.character(host), as.character(key),PACKAGE="doRedis")
  invisible()
}

.delOK <- function()
{
  .Call("delOK",PACKAGE="doRedis")
  invisible()
}

<<<<<<< HEAD
.workerInit <- function(expr, exportenv, packages, seed, log)
{
  tryCatch(
    {for (p in packages)
      library(p, character.only=TRUE)
    }, error=function(e) cat(as.character(e),'\n',file=log)
  )
# Override the function set.seed.worker in the exportenv to change!
=======
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
    }, error=function(e) cat(as.character(e),'\n',file=log)
  )
>>>>>>> devel
  assign('expr', expr, .doRedisGlobals)
  assign('exportenv', exportenv, .doRedisGlobals)
# XXX This use of parent.env should be changed. It's used here to
# set up a valid search path above the working evironment, but its use
# is fraglie as this may function be dropped in a future release of R.
# Would attach work instead?
  parent.env(.doRedisGlobals$exportenv) <- globalenv()
<<<<<<< HEAD
  tryCatch(
   {
    if(exists('set.seed.worker',envir=.doRedisGlobals$exportenv))
      do.call('set.seed.worker',list(seed),envir=.doRedisGlobals$exportenv)
    else if(!exists('initRNG', envir=.doRedisGlobals)) {
      set.seed((log10(as.numeric(seed))/308)*2^31)
      assign('initRNG', TRUE, envir=.doRedisGlobals)
    }
   },
   error=function(e) cat(as.character(e),'\n',file=log)
  )
# Look for and run generic worker initialization function...
  tryCatch(
   {
    if(exists("worker.init",envir=.doRedisGlobals$exportenv))
      do.call("worker.init",args=list(),envir=.doRedisGlobals$exportenv)
   },
   error=function(e) cat(as.character(e),'\n',file=log)
  )
=======
>>>>>>> devel
}

.evalWrapper <- function(args)
{
  env <- .doRedisGlobals$exportenv
  tryCatch({
      lapply(names(args), function(n)
<<<<<<< HEAD
                         assign(n, args[[n]], envir=env))
# Is this OK? Previous versions had a curious use of
# evalq(eval(...)). I'm not sure why.
eval(.doRedisGlobals$expr, envir=env)
=======
                         assign(n, args[[n]], pos=.doRedisGlobals$exportenv))
# eval(.doRedisGlobals$expr, envir=.doRedisGlobals$exportenv)
      if(exists(".Random.seed",envir=.doRedisGlobals$exportenv))
      {
        assign(".Random.seed",.doRedisGlobals$exportenv$.Random.seed, envir=globalenv())
      }
      tryCatch(
      {
# Override the function set.seed.worker to roll your own RNG.
        if(exists('set.seed.worker',envir=.doRedisGlobals$exportenv))
          do.call('set.seed.worker',list(0),envir=.doRedisGlobals$exportenv)
       }, error=function(e) cat(as.character(e),'\n',file=log))
      eval(.doRedisGlobals$expr, envir=.doRedisGlobals$exportenv)
     # evalq(eval(.doRedisGlobals$expr), envir=.doRedisGlobals$exportenv)
>>>>>>> devel
    },
    error=function(e) e  # Return the error to the master
  )
}

<<<<<<< HEAD
startLocalWorkers <- function(n, queue, host="localhost", port=6379,
  iter=Inf, timeout=30, log=stdout(),
  Rbin=paste(R.home(component='bin'),"R",sep="/"), password=NULL)
=======
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
  Rbin=paste(R.home(component='bin'),"R",sep="/"), password)
>>>>>>> devel
{
  m <- match.call()
  f <- formals()
  l <- m$log
  if(is.null(l)) l <- f$log
  cmd <- paste("require(doRedis);redisWorker(queue='",
<<<<<<< HEAD
         queue, "', host='", host,"', port=", port,", iter=", iter,", timeout=",
         timeout,", log=",deparse(l),", password='", password,"')",sep="")
=======
      queue, "', host='", host,"', port=", port,", iter=", iter,", timeout=",
      timeout,", log=",deparse(l),sep="")
  if(!missing(password)) cmd <- sprintf("%s,password='%s'",cmd,password)
  cmd <- sprintf("%s)",cmd)

>>>>>>> devel
  j=0
  args <- c("--slave","-e",paste("\"",cmd,"\"",sep=""))
  while(j<n) {
# system2(Rbin,args=args,wait=FALSE,stdout=NULL)
    system(paste(c(Rbin,args),collapse=" "),intern=FALSE,wait=FALSE)
    j = j + 1
  }
}

<<<<<<< HEAD
redisWorker <- function(queue,
                          host="localhost",
                          port=6379,
                          iter=Inf,
                          timeout=30,
                          log=stdout(),
                          connected=FALSE,
                          password=NULL)
=======
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
#'
#' @return NULL is invisibly returned.
#'
#' @seealso \code{\link{registerDoRedis}}, \code{\link{startLocalWorkers}}
#'
#' @export
redisWorker <- function(queue, host="localhost", port=6379, iter=Inf, timeout=30, log=stdout(), connected=FALSE, password=NULL)
>>>>>>> devel
{
  if(!is.null(password) && nchar(password)<1) password=c()
  if (!connected)
<<<<<<< HEAD
    redisConnect(host,port,password=password,nodelay=TRUE)
  assign(".jobID", "   ~~~   ", envir=.doRedisGlobals) # dummy id
  queueCounter <- sprintf("%s:counter",queue) # Job id counter
  SEED <- redisIncr(queueCounter) # Just to make sure this key exists, also
#   used as a seed in lieu of a better user-supplied pRNG. See workerInit.
  queueWorkers <- sprintf("%s:workers",queue)
  redisIncr(queueWorkers)
=======
    redisConnect(host,port,password=password)
  sink(type="message",append=TRUE,file=log)
  sink(type="output",append=TRUE,file=log)
  assign(".jobID", "0", envir=.doRedisGlobals)
  queueLive <- paste(queue,"live",sep=".")
  if(!redisExists(queueLive)) redisSet(queueLive, "")
  queueCount <- paste(queue,"count",sep=".")
  for(j in queueCount)
    tryCatch(redisIncr(j),error=function(e) invisible())
>>>>>>> devel
  cat("Waiting for doRedis jobs.\n", file=log)
  flush.console()
  k <- 0
  while(k < iter)
  {
    ID <- redisBLPop(queue,timeout=timeout)[[1]] # Retrieve a job ID
# We terminate the worker loop after a timeout when all specified work
# queues have been deleted.
    if(is.null(ID[[1]]))
     {
<<<<<<< HEAD
       ok <- redisExists(queueCounter)
       if(!ok) {
# If we get here, our entire job queue was deleted.
# Clean up and exit worker loop.
         removeQueue(queue)
         break
       }
     }
    else
     {
      queueEnv <- sprintf("%s:%.0f.env",queue,ID)
      queueResults <- sprintf("%s:%.0f.results",queue,ID)
      cat("Processing job ",ID," from queue ",queue,"\n")
=======
      ok <- FALSE
      for(j in queueLive) ok <- ok || redisExists(j)
      if(!ok) {
# If we get here, ALL our queues were deleted. Clean up and exit worker loop.
        for(j in queueOut) if(redisExists(j)) redisDelete(j)
        for(j in queueEnv) if(redisExists(j)) redisDelete(j)
        for(j in queueCount) if(redisExists(j)) redisDelete(j)
        for(j in queue) if(redisExists(j)) redisDelete(j)
        break
      }
     }
    else
     {
      k <- k + 1
      cat("Processing task(s)",names(work[[1]]$argsList),"from queue",names(work),"ID",work[[1]]$ID,"\n",file=log)
      flush.console()
>>>>>>> devel
# Check that the incoming work ID matches our current environment. If
# not, we need to re-initialize our work environment with data from the
# <queue>.env Redis string.
      if(get(".jobID", envir=.doRedisGlobals) != ID)
       {
        initdata <- redisGet(queueEnv)
        .workerInit(initdata$expr, initdata$exportenv, initdata$packages,
                    SEED,log)
        assign(".jobID", ID, envir=.doRedisGlobals)
       }
# Retrieve a task

# DEBUG: 'next' at this point simulates a bad failure--a task announcement
# pulled from the work queue, but no task pulled. This leaves
# total = queued + started + finished 
# out of balance.
#if(k>0) next

      task <- .doRedisGlobals$exportenv$.getTask(queue, ID, .getTag())
# Maybe we didn't get a task for some reason! Nobody likes me :(
# I'll put this job notice back into the work queue... This stall
# sucks though. Is there a better approach here?
# Note that a malicious or crashed R worker process might not do
# this. eventually the master process will detect an imbalance in the
# work queue anyway and correct things. But it's better to do it here.
      if(is.null(task))
      {
        ok <- redisExists(queueCounter)
        if(!ok) break         # This job is gone!
        redisRPush(queue, ID) # Put this job notice back.
        Sys.sleep(1)          # What's appropriate here?
        next
      }
      k <- k + 1
      cat("Processing task",task$task_id,"... from queue",queue,"jobID",ID,"\n",file=log)
      flush.console()
# Fault detection
      fttag.start <- sprintf("%s:%.0f.start.%s",queue,ID,task$task_id)
# The 'start' key has already been set for us by Redis. We set it again
# anyway just in case (for example, a custom getTask function might forget
# to do this).
      redisSet(fttag.start, task$task_id)
      fttag.alive <- sprintf("%s:%.0f.alive.%s",queue,ID,task$task_id)
# fttag.alive is a matching ephemeral key that is regularly kept alive by the
# setOK helper thread. Upon disruption of the thread (for example, a crash),
# the resulting Redis state will be an unmatched start tag, which may be used
# by fault tolerant code to resubmit the associated jobs.
      .setOK(port, host, fttag.alive)
# Now do the work.
# We assume that job order is encoded in names(args), cf. doRedis.
      result <- lapply(task$args, .evalWrapper)
      names(result) <- names(task$args)
      redisLPush(queueResults, result)
      tryCatch(redisDelete(fttag.start), error=invisible, warning=invisible)
      .delOK()
      tryCatch(redisDelete(fttag.alive), error=invisible, warning=invisible)
    }
  }
# Either the queue has been deleted, or we've exceeded the number of
# specified work iterations.
<<<<<<< HEAD
  redisDecr(queueWorkers)
  cat("Worker exit.\n", file=log)
  redisClose()
=======
  for(j in queueCount) if(redisExists(j)) redisDecr(j)
  cat("Worker exit.\n", file=log)
  if (!connected)
    redisClose()
>>>>>>> devel
}
