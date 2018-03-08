#!/usr/bin/env Rscript

options(java.parameters = "-Xmx5g")
library("spacyr")
## Finding a python executable with spacy installed...
## spaCy (language model: en) is installed in more than one python
## spacyr will use /anaconda/bin/python (because ask = FALSE)
## successfully initialized (spaCy Version: 2.0.1, language model: en

library(jwatjars)
library(rJava)
library(jwatr)
library(async)
library(MITIE)
spacy_initialize( python_executable = "/home/paul/anaconda3/envs/py36/bin/python3.6" ,model = "en", ask = F)
# spacy_initialize( python_executable = "/home/paul/anaconda3/envs/py36/bin/python3.6" ,model = "en_core_web_md", ask = F )

library(tidyverse)
library(base64url)
library(htmltidy)
library(jsonlite)
library(lettercase)
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
idx <- NULL
library(urltools)
library(rvest)


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


# #Go to client directory
library(doParallel)
library(doMC)
library(foreach)
#cl <- makeCluster(24)
#registerDoParallel(cl)
i = 4
j = 386
library(rredis)

#for (i in 1:nrow(xdf)) {


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


warcfilename <- paste(warc, filelist[i], sep = "/")

#CHECK B


library(async)
xdf <- NULL
xdf <- read_warc(warcfilename, warc_types = "response", include_payload = TRUE)
N <- nrow(xdf)  # some magic number, possibly an overestimate
  



foreach(j = 1:N, .combine = c, .packages = c("uuid" ,"stringi",  "boilerpipeR", "xml2", "tokenizers", "purrr", "rvest" )) %do%  {

  uuid <-  UUIDgenerate()
   
   
  }


  httpResp <-  unlist(stri_split_fixed(str = as.character(xdf$payload[[j]]), pattern = "\r\n\r\n", n = 2 ))  
  library(async)
  httpRespHeaders <- httpResp[1]
  httpRespBody <- httpResp[2]
  tidyHeaders <-  (httpRespHeaders )
  tidyBody <- httpRespBody

  #  if (!dir.exists(paste(pwd,i,j, sep="_")) ) {
  #    dir.create(paste(pwd,i,j, sep="_"), showWarnings = TRUE, recursive = FALSE, mode = "0777")
  #    print(paste("creating dir:", paste(pwd,i,j, sep="_"), sep = " " ))
  #  }

  tidyBody  %>% xmlElementsByTagName( recursive = T)
  xml2::read_html(tidyHeaders) 
  
  
  prettify(xml2::read_html(xdf$payload[[j]]))
  
  xdf$payload[[j]]
  
  url <- xdf$target_uri[[j]]
  base64 <- base64_urlencode(url)
  uuid_url <- UUIDgenerate(url)
  
  df <-  url %>%
    url_parse() %>% as.data.frame()
  library(SocialNetworks)
  
  murmaURL <- url %>% murmur3.32()
  library(jsonlite)
  library(httr)
  
  

  
  
  
  
  ############  
  
  for(j in N:1 ) {
   %>% map(~ iterator)%>% url_parse() %>% flatten() 
  
  df
  df2 <- df$domain %>% url_parse() 
  df3 <- c("murmer" = murmaURL,
           "base64" = base64,
           "uuid" = uuid)
  
  
}
  
  joined <-
    df3 %>% jsonlite::toJSON() %>% prettify( indent = 4)
  
  joined
  
  
  joined2 <- rbind(c("murmer" = murmaURL), c("uuid"  = uuid), c("base64" = base64), "payload" = c(str(joined)) )
  
  joined2
  
  rdf = as.data.frame( joined2, "payload" = c(stri_enc_detect(rawToChar(xdf$payload[[j]]))), dataframe = "columns")
  
  
  lang <- NULL
  
  library(iterators)
  
  ############## ASYNC LOOP
  ############## ASYNC LOOP
  ############## ASYNC LOOP
  ############## ASYNC LOOP
  ############## ASYNC LOOP
  
  
  
  romJSON <- function(x) jsonlite::fromJSON(x, simplifyVector = FALSE)
  revdep_authors <- async(function() {
    get_author <- function(package) {
      url <- paste0("https://crandb.r-pkg.org/", package)
      http_get(url)$
        then(~ fromJSON(rawToChar(.$content)))$
        then(~ .$Author)
    }
    
    gx <- http_get("https://crandb.r-pkg.org/-/topdeps/devel")$
      then(~ fromJSON(rawToChar(.$content)))$
      then(~ names(unlist(.)))$
      then(~ async_map(., get_author))
    
    await(gx)
  })
  synchronise(revdep_authors())[1:3]
  
  ############## ASYNC LOOP
  ############## ASYNC LOOP
  ############## ASYNC LOOP
  ############## ASYNC LOOP
  ############## ASYNC LOOP
  
  
  
  
  library(utils)
  
  
  library(lettercase)
  
  #### FILENAME CREATOR
  #### JSON SERIALISATION
  #### TERM FREQUENCY
  
  ####
  library(rethinker)
  
  
  cn<-openConnection()
  
  
  
  a <- set(c("div", "span","a","h1","h2","h3","h4", "h5", "p", "i", "b"))
  
  b <-set(c("h1","h2","h3","h4"))

  c <-set(c("div", "span", "p"))
  
  d <-set(c("i", "b", "div"))

  
  set.seed(1L)
  DF = data.frame(ID1 = sample(letters[1:2], 10, TRUE),
                  ID2 = sample(1:3, 10, TRUE),
                  val = sample(10),
                  stringsAsFactors = FALSE,
                  row.names = sample(LETTERS[1:10]))
  DF
  
  parsedTidyBody <- html_name  %>%  
    xml2::xml_find_all(xpath = ".//a") %>% 
    xml_child(read_html(encHTML, options = "RECOVER"))
  
  
  
  
  intersect()
  nsets <- set(c("a","b","c","d"))
  
  library("permute")
  
  shuffleSet(n, nset, control = how(), check = TRUE, quietly = FALSE)
  
  
  tagsList <- c("div", "span","a","h1","h2","h3","h4", "h5", "p", "i", "b")
  
  
  library(lettercase)
  
  (r1 <- shuffle(10))
  
  ## observations represent a spatial grid, 5rx4c
  
  nr <- 3
  nc <- 11
  CTRL <- how(within = Within(type = "grid", ncol = nc, nrow = nr))
  perms <- shuffle(100, control = CTRL)
  ## view the permutation as a grid
  matrix(matrix(1:100, nrow = nr, ncol = nc)[perms],
         ncol = nc, nrow = nr)
  ## spatial grids within each level of block, 4 x (5r x 5c)
  nr <- 5
  nc <- 5
  nb <- 4 ## number of blocks
  plots <- Plots(gl(nb, 25))
  CTRL <- how(plots = plots,
              within = Within(type = "grid", ncol = nc, nrow = nr))
  shuffle(100, CTRL)
  ## as above, but with same permutation for each level
  CTRL <- how(plots = plots,
              within = Within(type = "grid", ncol = nc, nrow = nr,
                              constant = TRUE))
  shuffle(100, CTRL)
  
  
  
  DX = NULL
  
  ########
  
  foreach(j = 1:N, .combine = c, .packages = c("uuid" ,"stringi",  "boilerpipeR", "xml2", "tokenizers", "purrr", "rvest" )) %dopar%  {
  }
  
  
  library(SocialMediaLab)
  
  library('rvest')
  
  require('doRedis')
  redisWorker('jobs')
  
  #  tidyBody
  #  tidyHeaders
  # httpRespBody
  # title
  # links 
  # classifier
  # language - encoding
  # tags
  
  sentences <- boilerpipeR::ArticleSentencesExtractor(httpRespBody)
  everything <- boilerpipeR::KeepEverythingExtractor(httpRespBody)
  
  
  
  
  encHTML <-as.character(unlist(stri_enc_toascii(tidyBody)))
  
  guess_encoding(encHTML)
  
  parsedTidyBody <- html_name(
    xml_child(read_html(httpRespBody, options = "RECOVER"))
  )
  parsedTidyBody <- html_name(
    xml_length(xml_child(read_html(encHTML, options = "RECOVER")))
  )
  
  parsedTidyBody <- html_name(
    xml_parent(xml_child(read_html(encHTML, options = "RECOVER")))
  )
  
  
  parsedTidyBody <- html_name  %>%  
    xml2::xml_find_all(xpath = ".//a") %>% 
    xml_child(read_html(encHTML, options = "RECOVER"))
  
  
  
  
  
  html_text(parsedTidyBody, trim = FALSE)
  
  (xmlToList(tidyBody))
  
  segment(boilerpipeR::ArticleSentencesExtractor(tidyBody), "paragraphs")
  
  sentencestidy <- tokenize_paragraphs(boilerpipeR::ArticleSentencesExtractor(tidyBody), paragraph_break = "")
  sentences <- boilerpipeR::ArticleSentencesExtractor(httpRespBody)
  library(data.tree)
  
  
  xml2::xml_find_all(parsedTidyBody)
  xml_structure(httpRespBody, indent = 2)
  
  html_structure(httpRespBody, indent = 2)
  rvest::html_text(httpRespBody)
  
  sentences <- boilerpipeR::ArticleSentencesExtractor(tidyBody)
  
  htmlPage <- xml2::read_html(tidyBody)
  htmlPage
  
  xmlRoot<- xml2::xml_root(htmlPage)
  library(spacyr)
  
  
  
  htmltextO <- rvest::html_text(htmlPage)
  
  
  
  
  html_text(htmltextO)        
  
  
  
  
  i = 4
  j = 300
  
  library(text2vec)
  library(data.table)
  library(magrittr)
  
  
  # define preprocessing function and tokenization function
  prep_fun = tolower
  tok_fun = word_tokenizer
  
  it_train = itoken(tidyBody, 
                    preprocessor = prep_fun, 
                    tokenizer = tok_fun, 
                    progressbar = T)
  
  vocab = create_vocabulary(it_train)
  
  vectorizer = vocab_vectorizer(vocab)
  
  t1 = Sys.time()
  dtm_train = create_dtm(it_train, vectorizer)
  print(difftime(Sys.time(), t1, units = 'sec'))
  
  identical(rownames(dtm_train), tidyBody)
  xml_children()
  
  article <- boilerpipeR::ArticleExtractor(tidyBody)
  sentences <- boilerpipeR::ArticleSentencesExtractor(httpRespBody) %>% tokenize_paragraphs() 
  
  httpRespBody
  
  para <- tokenize_paragraphs(x = sentences, strip_punctuation = FALSE, simplify = )  
  sent <- tokenize_sentences(x = sentences, strip_punctuation = FALSE, simplify = )    
  MITIE::NamedEntityExtractor(.xData = article)
  
  
  tags_div <- xml_find_all(htmlPage, ".//div")
  
  tags_span <- xml_find_all(htmlPage, ".//span")
  tags_a <- xml_find_all(htmlPage, ".//a")
  tags_a <- xml_find_all(htmlPage, ".//a")
  
  divs <- xml_find_all(htmlPage, ".//h1")
  xml2::xml_find_all(read_html(r), "//div")
  xml2::xml_find_all(read_html(r), ".//p")
  
  
  xml2::xml_find_all(read_html(r), ".//*")
  links <-  XML::getHTMLLinks(tidyBody)
  
  encBody
  
  httpRespBody
  
  
  tidyHTML1
  
  
  tidyHTML2
  
  tag_p <- htmlPage %>% html_nodes("p") %>%
    html_text() %>%
    map( ~ .x[.x != ''])
  
  
  x <- read_html(body, options = "RECOVER")
  print("  #CHECK 4... ")
  tag_p <- x %>% html_nodes("p") %>%
    html_text() %>%
    map( ~ .x[.x != ''])
  
  
  #####
  xdf$payload[[j]]
  
  str(unlist(cnn))
  
  u <- xdf$target_uri[[j]]
  
  # Find all vs find one -----------------------------------------------------
  
  print("  #CHECK B..1 ")
  DF <-
    data.frame(
      id = as.character("", N),
      i = rep("", N),
      j = rep("", N),
      url = rep("", N),
      stringsAsFactors = FALSE
    )
  
  Article <- boilerpipeR::ArticleExtractor(httpRespBody)
  
  httpRespBod
  
  para <- xml_find_all(read_html(r, options = "RECOVER"), ".//p")
  jsonlite::toJSON(para)
  
  # If you apply xml_find_all to a nodeset, it finds all matches,
  # de-duplicates them, and returns as a single list. This means you
  # never know how many results you'll get
  xml_find_all(para, ".//h*")
  
  # xml_find_first only returns the first match per input node. If there are 0
  # matches it will return a missing node
  xml_find_first(xml_find_first(para, ".//b"))
  xml_text(xml_find_first(para, ".//b"))
  
  x <- read_html(body, options = "RECOVER")
  
  tag_p <- x %>% html_nodes("./div") %>%
    html_text() %>%
    map( ~ .x[.x != ''])
  
  listcn <- c(cn1, cn2, cn3)
  
  require(stats)
  glmout <- capture.output(summary(glm(case ~ spontaneous+induced,
                                       data = infert, family = binomial())))
  glmout[1:5]
  capture.output(1+1, 2+2)
  capture.output({1+1; 2+2})
  
  ## Not run: ## on Unix-alike with a2ps available
  op <- options(useFancyQuotes=FALSE)
  pdf <- pipe("a2ps -o - | ps2pdf - tempout.pdf", "w")
  capture.output(example(glm), file = pdf)
  close(pdf); options(op) ; system("evince tempout.pdf &")
  
  ## End(Not run)
  
  
  if (!is.na(listcn)) {
    
    charnum <- min(listcn[!is.na(listcn)])
    #if (!is.na(charnum) && !is.null(charnum)) {
    body <- stri_sub(r, charnum + 30)
    
    if (!is.na(body)){

      
      r <- xdf$payload[[j]]
      
      
      print("empty")
      
      
      Article <- boilerpipeR::ArticleExtractor(body)
      
      
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
  
  
  gc()
  
  #   
  #   
  # }
  # 
  # 
  # 
  # 
  # stopCluster(cl)
  # 
  # 
  # Convert R objects to/from JSON
  # Description
  # These functions are used to convert between JSON data and R objects. The toJSON and fromJSON functions use a class based mapping, which follows conventions outlined in this paper: https://arxiv.org/abs/1403.2805 (also available as vignette).
  # 
  # Usage
  # fromJSON(txt, simplifyVector = TRUE, simplifyDataFrame = simplifyVector,
  #          simplifyMatrix = simplifyVector, flatten = FALSE, ...)
  # 
  # toJSON(x, dataframe = c("rows", "columns", "values"), matrix = c("rowmajor",
  #                                                                  "columnmajor"), Date = c("ISO8601", "epoch"), POSIXt = c("string",
  #                                                                                                                           "ISO8601", "epoch", "mongo"), factor = c("string", "integer"),
  #        complex = c("string", "list"), raw = c("base64", "hex", "mongo"),
  #        null = c("list", "null"), na = c("null", "string"), auto_unbox = FALSE,
  #        digits = 4, pretty = FALSE, force = FALSE, ...)
  # Arguments
  # txt	
  # a JSON string, URL or file
  # 
  # simplifyVector	
  # coerce JSON arrays containing only primitives into an atomic vector
  # 
  # simplifyDataFrame	
  # coerce JSON arrays containing only records (JSON objects) into a data frame
  # 
  # simplifyMatrix	
  # coerce JSON arrays containing vectors of equal mode and dimension into matrix or array
  # 
  # flatten	
  # automatically flatten nested data frames into a single non-nested data frame
  # 
  # ...	
  # arguments passed on to class specific print methods
  # 
  # x	
  # the object to be encoded
  # 
  # dataframe	
  # how to encode data.frame objects: must be one of 'rows', 'columns' or 'values'
  # 
  # matrix	
  # how to encode matrices and higher dimensional arrays: must be one of 'rowmajor' or 'columnmajor'.
  # 
  # Date	
  # how to encode Date objects: must be one of 'ISO8601' or 'epoch'
  # 
  # POSIXt	
  # how to encode POSIXt (datetime) objects: must be one of 'string', 'ISO8601', 'epoch' or 'mongo'
  # 
  # factor	
  # how to encode factor objects: must be one of 'string' or 'integer'
  # 
  # complex	
  # how to encode complex numbers: must be one of 'string' or 'list'
  # 
  # raw	
  # how to encode raw objects: must be one of 'base64', 'hex' or 'mongo'
  # 
  # null	
  # how to encode NULL values within a list: must be one of 'null' or 'list'
  # 
  # na	
  # how to print NA values: must be one of 'null' or 'string'. Defaults are class specific
  # 
  # auto_unbox	
  # automatically unbox all atomic vectors of length 1. It is usually safer to avoid this and instead use the unbox function to unbox individual elements. An exception is that objects of class AsIs (i.e. wrapped in I()) are not automatically unboxed. This is a way to mark single values as length-1 arrays.
  # 
  # digits	
  # max number of decimal digits to print for numeric values. Use I() to specify significant digits. Use NA for max precision.
  # 
  # pretty	
  # adds indentation whitespace to JSON output. Can be TRUE/FALSE or a number specifying the number of spaces to indent. See prettify
  # 
  # force	
  # unclass/skip objects of classes with no defined JSON mapping
  # 
  # Details
  # The toJSON and fromJSON functions are drop-in replacements for the identically named functions in packages rjson and RJSONIO. Our implementation uses an alternative, somewhat more consistent mapping between R objects and JSON strings.
  # 
  # The serializeJSON and unserializeJSON functions in this package use an alternative system to convert between R objects and JSON, which supports more classes but is much more verbose.
  # 
  # A JSON string is always unicode, using UTF-8 by default, hence there is usually no need to escape any characters. However, the JSON format does support escaping of unicode characters, which are encoded using a backslash followed by a lower case "u" and 4 hex characters, for example: "Z\u00FCrich". The fromJSON function will parse such escape sequences but it is usually preferable to encode unicode characters in JSON using native UTF-8 rather than escape sequences.
  # 
  # References
  # Jeroen Ooms (2014). The jsonlite Package: A Practical and Consistent Mapping Between JSON Data and R Objects. arXiv:1403.2805. https://arxiv.org/abs/1403.2805
  # 
  # Examples
  # # Stringify some data
  # jsoncars <- toJSON(mtcars, pretty=TRUE)
  # cat(jsoncars)
  # 
  # # Parse it back
  # fromJSON(jsoncars)
  # 
  # # Parse escaped unicode
  # fromJSON('{"city" : "Z\\u00FCrich"}')
  # 
  # # Decimal vs significant digits
  # toJSON(pi, digits=3)
  # toJSON(pi, digits=I(3))
  # 
  # ## Not run: retrieve data frame
  # data1 <- fromJSON("https://api.github.com/users/hadley/orgs")
  # names(data1)
  # data1$login
  # 
  # # Nested data frames:
  # data2 <- fromJSON("https://api.github.com/users/hadley/repos")
  # names(data2)
  # names(data2$owner)
  # data2$owner$login
  # 
  # # Flatten the data into a regular non-nested dataframe
  # names(flatten(data2))
  # 
  # # Flatten directly (more efficient):
  # data3 <- fromJSON("https://api.github.com/users/hadley/repos", flatten = TRUE)
  # identical(data3, flatten(data2))
  # 
  
  
  
  \
  
  install.packages("sand")
   
  