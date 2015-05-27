
storage config:

    {
      "type": "http",
      "connection": "http://btrabbit.com:8000",
      "resultKey": "results",
      "enabled": true
    }

samples:

    select name from (select name, length from http.`/e/api:search?q=avi` where $p=2) where length > 0
    select name, length from http.`/e/api:search` where $p=2 and $q='avi'
    select name, length from http.`/e/api:search?q=avi&p=2` where length > 0 
    
not support (still working on):

    select name from (select name, length from http.`/e/api:search` where $q='avi' and $p=2) where length > 0

