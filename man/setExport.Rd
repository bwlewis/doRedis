\name{setExport}
\alias{setExport}
\title{
setExport
}
\description{
Manually add symbol names to the worker environment export list.
}
\usage{
setExport(names = c())
}
\arguments{
  \item{names}{A vector of symbol names to export. }
}
\details{
The setExport function lets users manually declare symbol names
of corresponding objects that should be exported to workers.

The \code{foreach} function includes a similar \code{.export} parameter.

We provide this supplemental export option for users without direct access
to the \code{foreach} function, for example, when \code{foreach} is used
within a package.
}
\value{
Nothing is returned.
}
\author{
B. W. Lewis <blewis@illposed.net>
}

\examples{
\dontrun{
require("doRedis")
registerDoRedis("work queue")
startLocalWorkers(n=1, queue="work queue")

f <- function() pi

foreach(1) %dopar% eval(call("f"))
# Returns the error:
# Error in eval(call("f")) : task 1 failed - could not find function "f"

# Manuall export the symbol f:
setExport("f")
foreach(1) %dopar% eval(call("f"))
# Ok then.
#[[1]]
#[1] 3.141593
removeQueue("work queue")
}
}
