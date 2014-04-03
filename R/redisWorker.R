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
{
  .Call("setOK", as.integer(port), as.character(host), as.character(key),PACKAGE="doRedis")
  invisible()
}

.delOK <- function()
{
  .Call("delOK",PACKAGE="doRedis")
  invisible()
}

.workerInit <- function(expr, exportenv, packages, seed, log)
{
# Override the function set.seed.worker in the exportenv to change!
  assign('expr', expr, .doRedisGlobals)
  assign('exportenv', exportenv, .doRedisGlobals)
# XXX This use of parent.env should be changed. It's used here to
# set up a valid search path above the working evironment, but its use
# is fraglie as this may function be dropped in a future release of R.
# Would attach work instead?
  parent.env(.doRedisGlobals$exportenv) <- globalenv()
  tryCatch(
    {for (p in packages)
      library(p, character.only=TRUE)
    }, error=function(e) cat(as.character(e),'\n',file=log)
  )
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
}

.evalWrapper <- function(args)
{
  env <- .doRedisGlobals$exportenv
  tryCatch({
      lapply(names(args), function(n)
                         assign(n, args[[n]], envir=env))
# Is this OK? Previous versions had a curious use of
# evalq(eval(...)). I'm not sure why.
eval(.doRedisGlobals$expr, envir=env)
    },
    error=function(e) e  # Return the error to the master
  )
}

startLocalWorkers <- function(n, queue, host="localhost", port=6379,
  iter=Inf, timeout=30, log=stdout(),
  Rbin=paste(R.home(component='bin'),"R",sep="/"), password=NULL)
{
  m <- match.call()
  f <- formals()
  l <- m$log
  if(is.null(l)) l <- f$log
  cmd <- paste("require(doRedis);redisWorker(queue='",
         queue, "', host='", host,"', port=", port,", iter=", iter,", timeout=",
         timeout,", log=",deparse(l),", password='", password,"')",sep="")
  j=0
  args <- c("--slave","-e",paste("\"",cmd,"\"",sep=""))
  while(j<n) {
# system2(Rbin,args=args,wait=FALSE,stdout=NULL)
    system(paste(c(Rbin,args),collapse=" "),intern=FALSE,wait=FALSE)
    j = j + 1
  }
}

redisWorker <- function(queue,
                          host="localhost",
                          port=6379,
                          iter=Inf,
                          timeout=30,
                          log=stdout(),
                          connected=FALSE,
                          password=NULL)
{
  if(!is.null(password) && nchar(password)<1) password=c()
  if (!connected)
    redisConnect(host,port,password=password,nodelay=TRUE)
  assign(".jobID", "   ~~~   ", envir=.doRedisGlobals) # dummy id
  queueCounter <- sprintf("%s:counter",queue) # Job id counter
  SEED <- redisIncr(queueCounter) # Just to make sure this key exists, also
#   used as a seed in lieu of a better user-supplied pRNG. See workerInit.
  queueWorkers <- sprintf("%s:workers",queue)
  redisIncr(queueWorkers)
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
  redisDecr(queueWorkers)
  cat("Worker exit.\n", file=log)
  redisClose()
}
