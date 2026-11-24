// Package trailbeamsqs emits the spans of the Trailbeam span contract
// (contract/span-contract.md, schema version 3, revision 3.3) for every SQS call made through the AWS SDK
// for Go v2.
//
// One line where the AWS config is built is the whole setup:
//
//	cfg, err := config.LoadDefaultConfig(ctx)
//	trailbeamsqs.AppendMiddlewares(&cfg.APIOptions)
//	client := sqs.NewFromConfig(cfg)
//
// Every client made from cfg then emits one span per message sent or received, per receipt handle deleted
// or made visible again, and per PurgeQueue call. The package has its own always-on OpenTelemetry tracer
// provider, exporting OTLP over HTTP to TRAILBEAM_OTLP_TRACES_ENDPOINT, so the application's own sampler
// never drops a Trailbeam span. Call [Shutdown] before the program exits, so the last spans are exported.
//
// The middleware never returns an error into an SQS call, and it makes no SQS call of its own.
package trailbeamsqs
