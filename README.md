# Livy-Submit

With Livy-Submit, you can easily send pyspark files to run on your spark 
cluster in the case you don't have access to spark-submit on an edge-node
but you do have access to a Livy service on an edge-node.

You can find more information, including a sample session on https://www.guidodiepen.nl/2019/06/submitting-pyspark-jobs-to-livy-with-livy_submit/

## Prerequisites

The application is build using the requests library. It also can make use
of keyring to store the password for connecting to your edgenode (in order
for you not to have it hardcoded anywhere else).

