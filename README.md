# Taskhawk Go

[![Build Status](https://github.com/cloudchacho/taskhawk-go/checks)](https://github.com/cloudchacho/taskhawk-go/actions/workflows/gotest.yml/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/cloudchacho/taskhawk-go)](https://goreportcard.com/report/github.com/cloudchacho/taskhawk-go)
[![Godoc](https://godoc.org/github.com/cloudchacho/taskhawk-go?status.svg)](http://godoc.org/github.com/cloudchacho/taskhawk-go)
[![codecov](https://codecov.io/gh/cloudchacho/taskhawk-go/branch/main/graph/badge.svg?token=H6VWFF04JD)](https://codecov.io/gh/cloudchacho/taskhawk-go)

Taskhawk is an async task execution framework (à la celery) that works on AWS and GCP, while keeping
things pretty simple and straight forward. Any unbound function can be converted into a Taskhawk task.

Only Go 1.18+ is supported currently.

This project uses [semantic versioning](http://semver.org/).

## Quick Start

First, install the library:

```bash
go get github.com/cloudchacho/taskhawk-go
```

If your function takes multiple arguments, convert your function into a "Task" as shown here:

```go
type SendEmailTaskInput struct {...}

func SendEmail(ctx context.Context, input *SendEmailTaskInput) error {
    // send email
}
```

Tasks may accept input of arbitrary pointer type as long as it's serializable to JSON. Remember to export fields!

Then, define your backend:

```go
settings := aws.Settings{
    AWSAccessKey: <YOUR AWS ACCESS KEY>,
    AWSAccountID: <YOUR AWS ACCOUNT ID>,
    AWSRegion: <YOUR AWS REGION>,
    AWSSecretKey: <YOUR AWS SECRET KEY>,

    Queue: <YOUR TASKHAWK QUEUE>,
}
backend := aws.NewBackend(settings, nil)
```

Before the task can be dispatched, it would need to be registered, as shown below.

```go
hub := NewHub(Config{...}, backend)
task, err := taskhawk.RegisterTask(hub, "SendEmailTask", SendEmailTask)
```

And finally, dispatch your task asynchronously:

```go
task.dispatch(&SendEmailTaskInput{...})
```

## Development

### Prerequisites

Install go1.18.x

### Getting Started

Assuming that you have golang installed, set up your environment like so:

```bash

$ cd ${GOPATH}/src/github.com/cloudchacho/taskhawk-go
$ go build
```

### Running tests

```bash

$ make test  
# OR
$ go test -tags test ./...
```

## Getting Help

We use GitHub issues for tracking bugs and feature requests.

* If it turns out that you may have found a bug, please [open an issue](https://github.com/cloudchacho/taskhawk-go/issues/new>)

## Release notes

**Current version: v0.2.0-dev**

### v0.1.0

  - Initial version
