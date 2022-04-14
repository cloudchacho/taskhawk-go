/*
 * Copyright 2022, Cloudchacho
 * All rights reserved.
 */

/*
Package taskhawk is a replacement for celery that works on AWS and GCP, while keeping things pretty simple and straight
forward. Any unbound function can be converted into a Taskhawk task.

For inter-service messaging, see Hedwig: https://cloudchacho.github.io/hedwig/.

Provisioning

Taskhawk works on SQS or Pub/Sub as backing queues. Before you can publish tasks,
you need to provision the required infra. This may be done manually, or, preferably,
using Terraform. Taskhawk provides tools to make infra configuration easier: see
Taskhawk Terraform (https://github.com/cloudchacho/terraform-google-taskhawk) for further details.

Using Taskhawk

If your function takes multiple arguments, convert your function into a "Task" as shown here:

	type SendEmailTaskInput struct {...}

	func SendEmail(ctx context.Context, input *SendEmailTaskInput) error {
		// send email
	}

Tasks may accept input of arbitrary pointer type as long as it's serializable to JSON. Remember to export fields!

Then, define your backend:

	settings := aws.Settings{
		AWSAccessKey: <YOUR AWS ACCESS KEY>,
		AWSAccountID: <YOUR AWS ACCOUNT ID>,
		AWSRegion: <YOUR AWS REGION>,
		AWSSecretKey: <YOUR AWS SECRET KEY>,

		Queue: <YOUR TASKHAWK QUEUE>,
	}
	backend := aws.NewBackend(settings, nil)

Before the task can be dispatched, it would need to be registered, as shown below.

	hub := NewHub(Config{...}, backend)
	task, err := taskhawk.RegisterTask(hub, "SendEmailTask", SendEmailTask)

And finally, dispatch your task asynchronously:

	task.dispatch(ctx, &SendEmailTaskInput{...})

If you want to include a custom header with the message (for example, you can include a request_id field for
cross-application tracing), you can set it on the input object (HeadersCarrier interface).

If you want to customize priority, you can do it like so:

	task.dispatchWithPriority(ctx, &SendEmailTaskInput{...}, taskhawk.PriorityHigh)

Tasks are held in SQS queue / Pub/Sub subscription until they're successfully executed, or until they fail a
configurable number of times. Failed tasks are moved to a Dead Letter Queue, where they're
held for 14 days, and may be examined for further debugging.

Priority

Taskhawk provides 4 priority queues to use, which may be customized per task, or per message.
For more details, see https://godoc.org/github.com/cloudchacho/taskhawk-go/taskhawk#Priority.

Metadata and Headers

If your input struct satisfies `taskhawk.MetadataSetter` interface, it'll be filled in with the following attributes:

id: task identifier. This represents a run of a task.

priority: the priority this task message was dispatched with.

receipt: SQS receipt for the task. This may be used to extend message visibility if the task is running longer than
expected.

timestamp: task dispatch epoch timestamp

version: message format version.

If your input struct satisfies HeadersCarrier interface, it'll be filled with custom Taskhawk that the task
was dispatched with.

For a compile-time type assertion check, you may add (in global scope):

	var _ taskhawk.MetadataSetter = &SendEmailTaskInput{}
	var _ taskhawk.HeadersCarrier = &SendEmailTaskInput{}

This snippet won't consume memory or do anything at runtime.

consumer

A consumer for workers can be started as following:

	err := hub.ListenForMessages(ctx, &taskhawk.ListenRequest{...}, backend)

This is a blocking function, so if you want to listen to multiple priority queues,
you'll need to run these on separate goroutines.

For more complete code, see examples.
*/
package taskhawk
