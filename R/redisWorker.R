.doRedisGlobals <- new.env(parent=emptyenv())

# .setOK and .delOK support worker fault tolerance
`.setOK` <- function(port, host, key)
{
  .Call("setOK", as.integer(port), as.character(host), as.character(key),PACKAGE="doRedis")
  invisible()
}

`.delOK` <- function()
{
  .Call("delOK",PACKAGE="doRedis")
  invisible()
}

`.workerInit` <- function(expr, exportenv, packages, seed, log)
{
# Overried the function set.seed.worker in the exportenv to change!
  assign('expr', expr, .doRedisGlobals)
  assign('exportenv', exportenv, .doRedisGlobals)
# XXX This use of parent.env should be changed. It's used here to
# set up a valid search path above the working evironment, but its use
# is fraglie as this may function be dropped in a future release of R.
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
}

`.evalWrapper` <- function(args)
{
  tryCatch({
      lapply(names(args), function(n) 
                         assign(n, args[[n]], pos=.doRedisGlobals$exportenv))
      eval(.doRedisGlobals$expr, envir=.doRedisGlobals$exportenv)
    },
    error=function(e) e
  )
}

`startLocalWorkers` <- function(n, queue, host="localhost", port=6379, iter=Inf, timeout=60, log=stdout(), Rbin=paste(R.home(component='bin'),"R",sep="/"))
{
  m <- match.call()
  f <- formals()
  l <- m$log
  if(is.null(l)) l <- f$log 
  cmd <- paste("require(doRedis);redisWorker(queue='",queue,"', host='",host,"', port=",port,", iter=",iter,", timeout=",timeout,", log=",deparse(l),")",sep="")
  j=0
  args <- c("--slave","-e",paste("\"",cmd,"\"",sep=""))
  while(j<n) { 
#      system2(Rbin,args=args,wait=FALSE,stdout=NULL)
    system(paste(c(Rbin,args),collapse=" "),intern=FALSE,wait=FALSE)
    j = j + 1
  }
}

`redisWorker` <- function(queue, host="localhost", port=6379, iter=Inf, timeout=60, log=stdout())
{
  redisConnect(host,port)
  assign(".jobID", "0", envir=.doRedisGlobals)
  queueLive <- paste(queue,"live",sep=".")
  for(j in queueLive)
   {
    if(!redisExists(j)) redisSet(j,NULL)
   }
  queueCount <- paste(queue,"count",sep=".")
  for(j in queueCount)
    tryCatch(redisIncr(j),error=function(e) invisible())
  cat("Waiting for doRedis jobs.\n", file=log)
  flush.console()
  k <- 0
  while(k < iter) {
    work <- redisBLPop(queue,timeout=timeout)
    queueEnv <- paste(queue,"env", work[[1]]$ID, sep=".")
    queueOut <- paste(queue,"out", work[[1]]$ID, sep=".")
# We terminate the worker loop after a timeout when all specified work 
# queues have been deleted.
    if(is.null(work[[1]]))
     {
      ok <- FALSE
      for(j in queueLive) ok <- ok || redisExists(j)
      if(!ok) {
# If we get here, our queues were deleted. Clean up and exit worker loop.
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
      cat("Processing task",names(work[[1]]$argsList),"from queue",names(work),"ID",work[[1]]$ID,"\n",file=log)
      flush.console()
# Check that the incoming work ID matches our current environment. If
# not, we need to re-initialize our work environment with data from the
# <queue>.env Redis string.
      if(get(".jobID", envir=.doRedisGlobals) != work[[1]]$ID)
       {
        initdata <- redisGet(queueEnv)
        .workerInit(initdata$expr, initdata$exportenv, initdata$packages,
                    names(work[[1]]$argsList)[[1]],log)
        assign(".redisWorkerEnvironmentID", work$ID, envir=.doRedisGlobals)
       }
# XXX FT support
      fttag <- paste(names(work[[1]]$argsList),collapse="_")
      fttag.start <- paste(queue,"start",work[[1]]$ID,fttag,sep=".")
      fttag.alive <- paste(queue,"alive",work[[1]]$ID,fttag,sep=".")
# fttag.start is a permanent key
# fttag.alive is a matching ephemeral key that is regularly kept alive by the
# setOK helper thread. Upon disruption of the thread (for example, a crash),
# the resulting Redis state will be an unmatched start tag, which may be used
# by fault tolerant code to resubmit the associated jobs.
      redisSet(fttag.start,as.integer(names(work[[1]]$argsList)))
      .setOK(port, host, fttag.alive)
# Now do the work.
# XXX We assume that job order is encoded in names(argsList), cf. doRedis.
      result <- lapply(work[[1]]$argsList, .evalWrapper)
      names(result) <- names(work[[1]]$argsList)
      redisLPush(queueOut, result)
      tryCatch(redisDelete(fttag.start), error=function(e) invisible())
      .delOK()
    }
  }
# Either the queue has been deleted, or we've exceeded the number of
# specified work iterations.
  for(j in queueCount) if(redisExists(j)) redisDecr(j)
  cat("Worker exit.\n", file=log)
  redisClose()
}
