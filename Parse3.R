#!/usr/bin/env Rscript

options(java.parameters = "-Xmx5g")
Sys.setenv(R_HOME='/home/paul/R/x86_64-pc-linux-gnu-library/3.4')


library(jwatjars)
library(rJava)
library(jwatr)
library(magick)
library(tidyverse)
library(base64url)
library(htmltidy)
library(XML)
library(rvest)
library(httr)
library(boilerpipeR)
library(tidyverse)
library(tokenizers)
library(stringi)
library(tldextract)
library(hashFunction)
library(uuid)
library(xml2)
library(urltools)
library(purrr)
library(sparkwarc)                                  
library(sparklyr)
library(dplyr)  

idx <- NULL

Sys.setlocale('LC_ALL', 'C')

idx <- as.data.frame(idx)

setwd("/home/paul/wcrawl/ache/data/Bongers/data_pages/out")
pwd <- "/home/paul/wcrawl/ache/data/Bongers/data_pages/out"
warc <- "/home/paul/wcrawl/ache/data/Bongers/data_pages/WARC"

if (!dir.exists(pwd)) {
  dir.create(
    paste(pwd, "", sep = ""),
    showWarnings = TRUE,
    recursive = FALSE,
    mode = "0777"
  )
  print(pwd)
}else {
  cat("Already sorted")
}

# Do cleaning in preparation for the loop tings!

if (dir.exists(pwd)) {
  cat("Getting list of files")
  
  filelist  <-
    list.files(
      path = warc,
      pattern = "^crawl_data",
      all.files = FALSE,
      full.names = FALSE,
      recursive = FALSE,
      ignore.case = FALSE,
      include.dirs = FALSE,
      no.. = FALSE
    )
  numfiles <- length(filelist)
  
}

warcfilename <- paste(warc, filelist[i], sep = "/")

#CHECK B
xdf <- NULL



library(doMC)
library(foreach)
#cl <- makeCluster(24)
#registerDoParallel(cl)
i = 1
j = 48
library(rredis)
#require('doRedis')
#redisWorker('jobs')


#CHECK A
print("  #CHECK A...")
for (i in 1:nrow(xdf)) {
  print(paste("outer starts: i = ", i, sep = " "))
  #Check for logs dir...  and if it's not there, put it there.
  if (!dir.exists(paste(pwd, i, sep = "/"))) {
    dir.create(
      paste(pwd, i, sep = "/"),
      showWarnings = TRUE,
      recursive = FALSE,
      mode = "0777"
    )
    print(paste(i, paste(pwd, i, sep = "/"), sep = " "))
  }
  
  
  
  
  
  # text <- unlist(stri_split_fixed(str = strings, pattern = "<html>", n = 2))
  warcfilename <- paste(warc, filelist[i], sep = "/")
  
  #CHECK B
  xdf <- NULL
  
  xdf <- read_warc(warcfilename, warc_types = "response", include_payload = TRUE)

  
  print("  #CHECK B ")
  
  N <- nrow(xdf)  # some magic number, possibly an overestimate
  
  DF <-
     data.frame(
       id = as.character("", N),
       i = rep("", N),
       j = rep("", N),
       url = rep("", N),
       stringsAsFactors = FALSE
     )
  print(dim(DF))
  
  
  
  
  
  
  
  ##
  ##
  ##
  ## foreach(j = 1:N, .combine = c, .packages = c("uuid" ,"stringi",  "boilerpipeR", "xml2", "tokenizers", "purrr", "rvest" )) %dopar%  {
    # for (j in 1:N) {
    
  foreach(j = 1:N, .combine = c) %do%  {
    
    print("  #CHECK B..3 ")
    print(j)
    id <-  UUIDgenerate(use.time = TRUE)
    print(id)
    
   
   
   mainTags <- list("a", "div", "p", "title", "h1", "h2", "h3", "h4", "h5")
    
   
    
   for (i in seq(along=murmur32Input)) {
     murmur32 <- digest(murmur32Input[i], algo="murmur32", serialize=FALSE)
     cat(murmur32, "\n")
     stopifnot(identical(murmur32, murmur32Output[i]))
   }
    
    tab_warc <- glimpse(xdf)
    
    select(tab_warc, content_length, http_raw_headers)
    
     url = xdf$target_uri[[j]]
     ctype = xdf$http_protocol_content_type[[j]]
     headers = xdf$http_raw_headers[[j]]
   
     payload <- xdf$payload[[j]]
    
     class(payload)
     
     content(x = xdf$payload[[j]], "raw")
     
     charResponse <- rawToChar(payload, multiple = FALSE) 
  

     http_status(charResponse)     
     content(payload, "raw")
     library(stringi)
     stringi::stri_enc_detect(content(payload, "raw"))
     
     http_status(payload)
     content(xdf, "raw")
     
     GET("https://rud.is/test/untidy.html")
     
     
     
     res <- GET("https://rud.is/test/untidy.html")
     cat(content(res, as="text"))
     
     urlEncoded <- base64_urlencode(url)
    
     res <- GET("https://rud.is/test/untidy.html")
     cat(content(res, as="text"))
     


     library(sparkwarc)                                  # Load extension to read warc files
     library(sparklyr)                                   # Load sparklyr to use Spark from R
     library(dplyr)                                      # Load dplyr to perform analysis
     
     spark_install()                                     # Install Apache Spark
     
     config <- spark_config()                            # Create a config to tune memory
     config[["sparklyr.shell.driver-memory"]] <- "10G"   # Set driver memory to 10GB
     
     sc <- spark_connect(master = "local",               # Connecto to local cluster
                         config = config)                # using custom configs
     
     file <- gsub("s3n://commoncrawl/",                  # mapping the S3 bucket url
                  "http://commoncrawl.amazonaws.com/",   # into a adownloadable url
                  sparkwarc::cc_warc(1), "warc.gz")   # from the first archive file


     spark_read_warc(                                    # Read the warc file
       sc,                                               # into the sc Spark connection
       "warc",                                           # save into 'warc' table
       "warc.gz",                                        # loading from remote gz file
       repartition = 8,                                  # partition into 8 to maximize MBP cores
       parse = TRUE)                                     # parse tags and attributes
    

tbl(sc, "warc") %>% summarize(count = n())          # Count tags and attributes
     
     
     warc_stream_in(warcfilename, package="jwatr")
  
    pc <- payload[payload != ""]
    
    
    pc[pc == as.raw(0)] = as.raw(0x20) ## replace with 0x20 = <space>
    print("  #CHECK 2..c ")
    
    char <-  rawToChar(xdf$payload[[j]])
    
    r = stri_enc_toutf8(rawToChar(xdf$payload[[j]]))
    
    cnn <- stri_locate_first_fixed(r, "\r\n\r\n")
    
    stringi::stri_enc_detect(content(char, "parsed"))
    #####
    content(xdf$payload[[j]], "raw")
    r = stri_enc_toutf8(rawToChar(xdf$payload[[j]]))
    
    #####
    xdf$payload[[j]]
    
    str(unlist(cnn))
    
    u <- xdf$target_uri[[j]]
    
    
    
    cnn1 <- NULL
    cnn2 <- NULL
    cnn3 <- NULL
    
    cnn1 <- stri_locate_first_fixed(r, 'DOCTYPE')
    if (is.na(cnn1)) {
      cnn2 <- stri_locate_first_fixed(r, 'html>')
      if (is.na(cnn2)){
        cnn3 <- stri_locate_first_fixed(r, '<html')
        if (is.na(cnn3)){
          print("gonna need to dig deeper")
          #  break
        }
      }
    }
    
    cn1 <- NULL
    cn2 <- NULL
    cn3 <- NULL
    
    cn1 <- cnn1[1]
    cn2 <- cnn2[1]
    cn3 <- cnn3[1]
    
    
    
    
    
    listcn <- c(cn1, cn2, cn3)
    
    if (!is.na(listcn)) {
      
      charnum <- min(listcn[!is.na(listcn)])
      #if (!is.na(charnum) && !is.null(charnum)) {
      body <- stri_sub(r, charnum + 30)
      
      if (!is.na(body)){
        
        
        
        print("empty")
        
        
        Article <- boilerpipeR::ArticleExtractor(body)
        
        
        
        print(paste("NOT NULL", "url: ", u, sep=" "))
        
        
        write(Article, paste(pwd, i, paste(paste(i, "_article_", j, ".txt", sep = "")), sep = "/"))  
        
        
        
        x <- read_html(body, options = "RECOVER")
        print("  #CHECK 4... ")
        tag_p <- x %>% html_nodes("p") %>%
          html_text() %>%
          map( ~ .x[.x != ''])
        
        tag_h1 <- x %>% html_nodes("h1") %>%
          html_text() %>%
          map( ~ .x[.x != ''])
        
        tag_h2 <- x %>% html_nodes("h2") %>%
          html_text() %>%
          map( ~ .x[.x != ''])
        
        tag_h3 <- x %>% html_nodes("h3") %>%
          html_text() %>%
          map( ~ .x[.x != ''])
        
        tag_h4 <- x %>% html_nodes("h4") %>%
          html_text() %>%
          map( ~ .x[.x != ''])
        
        print("  #CHECK 5... ")
        
        tags <- list(tag_h1, tag_h2, tag_h3, tag_h4, tag_p)
        
        #   sent <- tokenize_sentences(Article)
        # sentlength <- length(sent)
        #   para <- tokenize_paragraphs(Article,
        # paragraph_break = "\n",
        # simplify = TRUE)
        # paralength <- length(para)
        
        
        
        print(paste("IS NULL *******", "url: ", u, sep = " "))
      } 
      
    } else {
      
    }
    
    
    
    
    
  } 
  
  gc()
  
  
  
}




stopCluster(cl)
