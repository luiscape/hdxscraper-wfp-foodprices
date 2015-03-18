## Functions to manipulate the status of
## ScraperWiki boxes.

changeSwStatus <- function(type = NULL, message = NULL, verbose = F) {
  if (!is.null(message)) { content = paste("type=", type, "&message=", message, sep="") }
  else content = paste("type=", type, sep="")
  curlPerform(postfields = content, url = 'https://scraperwiki.com/api/status', post = 1L)

  if (verbose == T) {
    cat(content)
  }
}
