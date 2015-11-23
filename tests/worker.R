require(doRedis)
require(rredis)

compare <- function(x, y, label="unexpected result")
{
  if(!isTRUE(all.equal(x, y))) stop(label)
}

# covr can't detect coverage of worker routines run in separate
# processes. This example test explicitly constructs a work queue
# and runs a worker locally to test it.
if(Sys.getenv("TEST_DOREDIS") == "TRUE")
{
  redisConnect()
  redisFlushAll()
  IDfile <- tempfile("doRedis")
  zz <- file(IDfile,"w")
  close(zz)
  ID <- basename(IDfile)
  ID <- gsub("\\\\","_",ID)
  queue <- "jobs"
  queueEnv <- paste(queue,"env", ID, sep=".")
  queueOut <- paste(queue,"out", ID, sep=".")
  queueStart <- paste(queue,"start",ID, sep=".")
  queueStart <- paste(queueStart, "*", sep="")
  queueAlive <- paste(queue,"alive",ID, sep=".")
  queueAlive <- paste(queueAlive, "*", sep="")
  redisSet(queueEnv, list(expr=as.symbol("j"), exportenv=new.env(), packages=c()))
  block <- list(`1`=1)
  redisRPush(queue, list(ID=ID, argsList=block))

  redisWorker("jobs", iter=1, timeout=1)
  redisFlushAll()
}
