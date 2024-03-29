% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/doRedis.R
\name{setExport}
\alias{setExport}
\title{Manually add symbol names to the worker environment export list.}
\usage{
setExport(names = c())
}
\arguments{
\item{names}{A character vector of symbol names to export.}
}
\value{
The value of \code{names} is invisibly returned (this function is used ofr its side effect).
}
\description{
The setExport function lets users manually declare symbol names
of corresponding objects that should be exported to workers.
}
\details{
The \code{foreach} function includes a similar \code{.export} parameter.

We provide this supplemental export option for users without direct access
to the \code{foreach} function, for example, when \code{foreach} is used
inside another package.
}
\examples{
\dontrun{
registerDoRedis("work queue")
startLocalWorkers(n=1, queue="work queue", linger=1)

f <- function() pi

(foreach(1) \%dopar\% tryCatch(eval(call("f")), error = as.character))
# Returns the error converted to a message:
# Error in eval(call("f")) : task 1 failed - could not find function "f"

# Manually export the symbol f:
setExport("f")
(foreach(1) \%dopar\% eval(call("f")))
# Now f is found.

removeQueue("work queue")
}

}
