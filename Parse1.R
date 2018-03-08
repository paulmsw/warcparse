

options(java.parameters = "-Xmx4g")

library(rJava)

library(jwatjars)
library(jwatr)
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
library(cleanNLP)
idx <- NULL

library(rvest)
library(purrr)

# current verison
packageVersion("jwatr")


Sys.setlocale('LC_ALL', 'C')



idx <- as.data.frame(idx)

setwd("/home/paul/work/BongersData/data_pages/out")
pwd <- "/home/paul/work/BongersData/data_pages/out"
warc <- "/home/paul/work/BongersData/data_pages/WARC"

if (!dir.exists(pwd)) {
  dir.create(
    paste(pwd, "", sep = ""),
    showWarnings = TRUE,
    recursive = FALSE,
    mode = "0777"
  )
  print(pwd)
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



# #Go to client directory
library(doParallel)
library(foreach)
cl <- makeCluster(24)
registerDoParallel(cl)
i = 1
j = 1


print(paste("outer starts: i = ", i, sep=" "))
#Check for logs dir...  and if it's not there, put it there.
if (!dir.exists(paste(pwd,i, sep="/"))) {
  dir.create(paste(pwd,i, sep="/"), showWarnings = TRUE, recursive = FALSE, mode = "0777")
  print(paste(i, paste(pwd,i, sep="/"), sep = " " ))
} 



#################################CHECK A
print("  #OUTER START...")
for (i in 1:numfiles) {
  
  
  
  # text <- unlist(stri_split_fixed(str = strings, pattern = "<html>", n = 2))
  warcfilename <- paste(warc, filelist[i], sep = "/")
  xdf <- read_warc(warcfilename, warc_types = "response", include_payload = TRUE)
  xdf <- bind_rows(xdf)
  
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
  
  foreach(j = 1:N, .combine = c) %do%  {
  

    
    
    id <-  UUIDgenerate(use.time = TRUE)
    
    #  if (!dir.exists(paste(pwd,i,j, sep="_")) ) {
    #    dir.create(paste(pwd,i,j, sep="_"), showWarnings = TRUE, recursive = FALSE, mode = "0777")
    #    print(paste("creating dir:", paste(pwd,i,j, sep="_"), sep = " " ))
    #  }
    
    
    payload <- (xdf$payload[[j]])
    
    
    pc <- payload[payload != ""]
    pc[pc == as.raw(0)] = as.raw(0x20) ## replace with 0x20 = <space>
    print("  #CHECK 2..c ")
    
    
    r = stri_enc_toutf8(rawToChar(xdf$payload[[j]]))
    
    
    u <- xdf$target_uri[[j]]
    
    
    
    
    
    
    
    DF[j, 1] <- id
    DF[j, 2] <- i
    DF[j, 3] <- j
    DF[j, 4] <- u
    
    
    
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
    
    cn1 <- cnn1[1] +30
    cn2 <- cnn2[1] +3
    cn3 <- cnn3[1] +3
    
    
    
    listcn <- c(cn1, cn2, cn3)
    
    if (!is.na(listcn)) {
      
      charnum <- min(listcn[!is.na(listcn)])
      #if (!is.na(charnum) && !is.null(charnum)) {
      body <- stri_sub(r, charnum + 30)
      
      if (!is.na(body)){
        
        
        
        print("empty")
        
        
        Article <- boilerpipeR::ArticleExtractor(body)
        
        
        
        print(paste("NOT NULL", "url: ", u, sep=" "))
        
        
        
        
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
        
        sent <- tokenize_sentences(Article)
        
        
        sentlength <- length(sent)
        para <- tokenize_paragraphs(Article,
                                    paragraph_break = "\n",
                                    simplify = TRUE)
        paralength <- length(para)
        parastruct <- list(sent, sentlength, para, paralength)
        structure <- list(tags, parastruct) 
        print(paste("IS NULL *******", "url: ", u, sep = " "))
        
        library(coreNLP)
        
        initCoreNLP("/home/paul/CoreNLP/stanford-ner-2017-06-09")
        output = annotateString(Article, format = "obj")
        
        getDependency(output)
        
        paraif (!dir.exists(paste(pwd,i, sep="/"))) {
          dir.create( paste(pwd, paste(paste(i, "_structure_", j, sep = "")), sep = ""), showWarnings = TRUE, recursive = FALSE, mode = "0777")
          print( paste(pwd, i, paste(paste(i, "_structure_", j, sep = "")), sep = "/"))
        } 
        
        paste(pwd, i, paste(paste(i, "_article_", j, ".txt", sep = "")), sep = "/")
      } 
      
    } else {
      
    }
    
    
    
    
    
  } 
  
  gc()
  
  
  
}




stopCluster(cl)