# Examples

The modules in this directory let you run Taskhawk with a real backend.

## Setup

1. Install Golang 1.16+
1. Build package
   ```shell script
   $ go build .
   ```

### Google

1. Install [gcloud](https://cloud.google.com/sdk/gcloud)
1. Authenticate with gcloud:
   ```shell script
   $ gcloud auth application-default login
   ``` 
1. Configure project:
    ```shell script
    $ gcloud config set project <GCP_PROJECT_ID>
    $ gcloud pubsub topics create taskhawk-dev-myapp-dlq
    $ gcloud pubsub subscriptions create taskhawk-dev-myapp-dlq --topic taskhawk-dev-myapp-dlq
    $ gcloud pubsub topics create taskhawk-dev-myapp
    $ gcloud pubsub subscriptions create taskhawk-dev-myapp --topic taskhawk-dev-myapp --dead-letter-topic taskhawk-dev-myapp-dlq
    ```

### AWS

1. Install [awscli](https://aws.amazon.com/cli/)
1. Authenticate with AWS:
   ```shell script
   $ aws configure
   ```
1. Configure project:
    ```shell script
    $ AWS_REGION=$(aws configure get region)
    $ AWS_ACCOUNT_ID=$(aws sts get-caller-identity | jq -r '.Account')
    $ aws sqs create-queue --queue-name TASKHAWK-DEV-MYAPP
    $ aws sqs create-queue --queue-name TASKHAWK-DEV-MYAPP-DLQ
    $ aws sqs set-queue-attributes --queue-url https://$AWS_REGION.queue.amazonaws.com/$AWS_ACCOUNT_ID/TASKHAWK-DEV-MYAPP --attributes "{\"Policy\":\"{\\\"Version\\\":\\\"2012-10-17\\\",\\\"Statement\\\":[{\\\"Action\\\":[\\\"sqs:SendMessage\\\",\\\"sqs:SendMessageBatch\\\"],\\\"Effect\\\":\\\"Allow\\\",\\\"Resource\\\":\\\"arn:aws:sqs:$AWS_REGION:$AWS_ACCOUNT_ID:TASKHAWK-DEV-MYAPP\\\",\\\"Principal\\\":{\\\"Service\\\":[\\\"sns.amazonaws.com\\\"]}}]}\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"arn:aws:sqs:$AWS_REGION:$AWS_ACCOUNT_ID:TASKHAWK-DEV-MYAPP-DLQ\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"}"
    ```

## Run

Publisher: (publishes 5 messages)

```shell script
$ go run . publisher
```

Consumer: (blocking command)

```shell script
$ go run . consumer
```

To use AWS:

```shell script
$ AWS_REGION=$(aws configure get region)
$ AWS_ACCOUNT_ID=$(aws sts get-caller-identity | jq -r '.Account')
$ AWS_REGION=$AWS_REGION AWS_ACCOUNT_ID=$AWS_ACCOUNT_ID SETTINGS_MODULE=example_aws_settings go run . publisher
```
