import boto3
import json
import os
from time import sleep

BUCKET_NAME = 'test'
  FILE_NAME = 'settings.json'

def main():
  executionId = os.getenv("executionId", None )
  clusterName = os.getenv("clusterName", None)
  clusterType = os.getenv("clusterType", None)
  nodeType = os.getenv("nodeType", None)
  dbName = os.getenv("dbName", None)
  userName = os.getenv("userName", None)
  userPassword = os.getenv("userPassword", None)
  region = os.getenv("region", None)
  numberOfNodes= os.getenv("numberOfNodes", None)

  client = boto3.client('redshift', region_name=region)
  if clusterName == None and executionId != None:
    taskId = executionId.split(":")[-1]
    clusterName = "rs-"+taskId.split("-")[0]

  if clusterType == 'single-node':
      client.create_cluster(
          DBName=dbName,
          ClusterIdentifier=clusterName,
          ClusterType=clusterType,
          NodeType=nodeType,
          MasterUsername=userName,
          MasterUserPassword=userPassword
      )
  else:
      client.create_cluster(
          DBName=dbName,
          ClusterIdentifier=clusterName,
          ClusterType=clusterType,
          NodeType=nodeType,
          MasterUsername=userName,
          MasterUserPassword=userPassword,
          NumberOfNodes=numberOfNodes
      )

  count = 0
  address = None
  while True:
      response = client.describe_clusters(
          ClusterIdentifier=clusterName
      )

      clusterInfo = response['Clusters'][0]
      status = clusterInfo['ClusterStatus']

      if "Endpoint" in clusterInfo:
          endpoint = clusterInfo["Endpoint"]
          address = endpoint["Address"]

      if (status == "available" and address != None):
          break
      count += 1
      sleep(5)

  context = {
      "executionId": executionId,
      "redshifts": [{
          "id": clusterName,
          "publicIp": address,
          "username": userName,
          "password": userPassword
      }]
  }
  setContextToS3(context)

def setContextToS3(context):
  s3 = boto3.client('s3')
  s3Resource = boto3.resource('s3')
  objectKey = context["executionId"] + "/" + FILE_NAME
  try:
      obj = s3.get_object(Bucket=BUCKET_NAME, Key=objectKey)
      savedContext = json.loads(obj['Body'].read())
      for key in context:
          if key in savedContext:
              if key == "executionId":
                  savedContext[key] = context[key]
              else:
                  savedContext[key] = savedContext[key] + context[key]

      print(savedContext)
      s3object = s3Resource.Object(BUCKET_NAME, objectKey)
      s3object.put(
          Body=(bytes(json.dumps(savedContext).encode('UTF-8')))
      )

  except Exception as e:
      s3object = s3Resource.Object(BUCKET_NAME, objectKey)
      s3object.put(
          Body=(bytes(json.dumps(context).encode('UTF-8')))
      )

main()