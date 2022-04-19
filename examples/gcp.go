package main

import (
	"os"

	gcloudPropagator "github.com/GoogleCloudPlatform/opentelemetry-operations-go/propagator"

	"github.com/cloudchacho/taskhawk-go/gcp"
)

const (
	gcpQueueName = "dev-myapp"
)

func gcpBackendSettings() gcp.Settings {
	return gcp.Settings{
		GoogleCloudProject: os.Getenv("GOOGLE_CLOUD_PROJECT"),
		QueueName:          gcpQueueName,
	}
}

func gcpBackend() *gcp.Backend {
	return gcp.NewBackend(gcpBackendSettings(), nil)
}

func gcpPropagator() gcloudPropagator.CloudTraceFormatPropagator {
	return gcloudPropagator.New()
}
