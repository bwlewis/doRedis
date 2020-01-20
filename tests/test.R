require(doRedis)

compare <- function(x, y, label="unexpected result")
{
  if(!isTRUE(all.equal(x, y))) stop(label)
}

if(Sys.getenv("TEST_DOREDIS") == "TRUE")
{
  setProgress(TRUE)
  setFtinterval(10)
# Basic test with two local worker processes
  queue <- "jobs"
  redisConnect() # need to connect to Redis before removing a queue
  removeQueue(queue)
  startLocalWorkers(n=2, queue, timeout=1)
  registerDoRedis(queue)
  ans <- foreach(j=1:10, .combine=sum) %dopar% j
  compare(ans, 55, "foreach")

# setX tests
  x <- 0
  setExport("x")
  setChunkSize(5)
  ans <- foreach(j=1:10, .combine=sum, .noexport="x") %dopar% {
    j + x
  }
  compare(ans, 55, "foreach")

  getDoParWorkers()

# Shut down
  removeQueue(queue)
  Sys.sleep(2)  # Allow time for workers to terminate
}
