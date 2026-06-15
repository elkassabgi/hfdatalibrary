# hfdatalibrary — R client for the HF Data Library
#
# Quick start:
#   library(hfdatalibrary)
#   hfdl_set_key("YOUR_API_KEY")          # or set env var HFDL_API_KEY
#   syms  <- hfdl_symbols()
#   aapl  <- hfdl_get("AAPL")             # clean 1-minute bars -> data.frame
#   daily <- hfdl_get("AAPL", version = "raw", timeframe = "daily")
#
# Data is survivorship-biased (universe fixed ~2023; pre-2022 survivor-
# conditioned). See https://hfdatalibrary.com/pages/docs for limitations.

.hfdl_env <- new.env(parent = emptyenv())
.hfdl_env$base_url <- Sys.getenv("HFDL_BASE_URL", "https://api.hfdatalibrary.com")
.hfdl_env$api_key  <- Sys.getenv("HFDL_API_KEY", "")

.HFDL_VERSIONS   <- c("clean", "raw")
.HFDL_TIMEFRAMES <- c("1min", "5min", "15min", "30min", "hourly", "daily", "weekly", "monthly")

#' Set the HF Data Library API key for this session.
#' @param api_key Your API key (from https://hfdatalibrary.com/pages/account).
#' @export
hfdl_set_key <- function(api_key) {
  stopifnot(is.character(api_key), nchar(api_key) > 0)
  .hfdl_env$api_key <- api_key
  invisible(TRUE)
}

.hfdl_request <- function(path, query = list(), auth = TRUE, timeout_s = 120, max_retries = 3) {
  url <- paste0(.hfdl_env$base_url, path)
  headers <- httr::add_headers(`User-Agent` = "hfdatalibrary-r/0.1.0")
  if (auth) {
    if (!nzchar(.hfdl_env$api_key)) {
      stop("No API key set. Call hfdl_set_key('...') or set HFDL_API_KEY. ",
           "Get a key at https://hfdatalibrary.com/pages/account", call. = FALSE)
    }
    headers <- httr::add_headers(`User-Agent` = "hfdatalibrary-r/0.1.0",
                                 `X-API-Key` = .hfdl_env$api_key)
  }
  last_err <- NULL
  for (attempt in seq_len(max_retries)) {
    resp <- tryCatch(
      httr::GET(url, headers, query = query, httr::timeout(timeout_s)),
      error = function(e) { last_err <<- e; NULL }
    )
    if (is.null(resp)) { Sys.sleep(1.5 * attempt); next }
    code <- httr::status_code(resp)
    if (code == 200) return(resp)
    if (code %in% c(401, 403)) stop("Authentication failed (", code, "). Check your API key.", call. = FALSE)
    if (code == 404) stop("Not found: ", path, " (check ticker/timeframe/version).", call. = FALSE)
    if (code == 429) { Sys.sleep(5 * attempt); next }
    if (code >= 500) { last_err <- paste("server error", code); Sys.sleep(2 * attempt); next }
    stop("HTTP ", code, call. = FALSE)
  }
  stop("Request to ", path, " failed after ", max_retries, " attempts: ",
       if (is.null(last_err)) "unknown" else last_err, call. = FALSE)
}

#' List available ticker symbols (no API key required).
#' @return Character vector of tickers.
#' @export
hfdl_symbols <- function() {
  resp <- .hfdl_request("/v1/symbols", auth = FALSE)
  data <- httr::content(resp, as = "parsed", type = "application/json")
  syms <- if (!is.null(data$symbols)) data$symbols else data
  vapply(syms, function(s) if (is.list(s)) s$ticker else s, character(1))
}

#' Fetch bars for one or many tickers.
#'
#' @param ticker    A ticker string, or a character vector of tickers.
#' @param version   "clean" (default) or "raw".
#' @param timeframe One of 1min,5min,15min,30min,hourly,daily,weekly,monthly.
#' @param format    "parquet" (default, needs the arrow package) or "csv".
#' @return A data.frame for one ticker, or a named list of data.frames for many.
#' @export
hfdl_get <- function(ticker, version = "clean", timeframe = "1min", format = "parquet") {
  if (!version %in% .HFDL_VERSIONS) stop("version must be one of: ", paste(.HFDL_VERSIONS, collapse = ", "), call. = FALSE)
  if (!timeframe %in% .HFDL_TIMEFRAMES) stop("timeframe must be one of: ", paste(.HFDL_TIMEFRAMES, collapse = ", "), call. = FALSE)
  if (!format %in% c("parquet", "csv")) stop("format must be 'parquet' or 'csv'", call. = FALSE)

  if (length(ticker) > 1) {
    out <- lapply(ticker, function(t) hfdl_get(t, version, timeframe, format))
    names(out) <- toupper(ticker)
    return(out)
  }

  resp <- .hfdl_request(
    paste0("/v1/download/", toupper(ticker)),
    query = list(version = version, timeframe = timeframe, format = format)
  )
  raw <- httr::content(resp, as = "raw")

  if (identical(format, "csv")) {
    return(utils::read.csv(text = rawToChar(raw), stringsAsFactors = FALSE))
  }
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("The 'arrow' package is required to read parquet. ",
         "Install it (install.packages('arrow')) or call hfdl_get(..., format = 'csv').", call. = FALSE)
  }
  tf <- tempfile(fileext = ".parquet")
  on.exit(unlink(tf), add = TRUE)
  writeBin(raw, tf)
  as.data.frame(arrow::read_parquet(tf))
}
