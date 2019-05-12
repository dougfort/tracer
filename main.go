package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"

	"go.opencensus.io/examples/exporter"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
)

var (
	// frontendKey allows us to breakdown the recorded data
	// by the frontend used when uploading the video.
	frontendKey tag.Key

	// videoSize will measure the size of processed videos.
	videoSize *stats.Int64Measure
)

func main() {
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	logger := zerolog.New(os.Stdout).
		With().Timestamp().Str("program", "tracer").Logger()
	logger.Info().Msg("Starting Tracer")

	ctx := context.Background()

	// Register an exporter to be able to retrieve
	// the data from the subscribed views.
	e, err := exporter.NewLogExporter(exporter.Options{ReportingInterval: time.Duration(time.Second)})
	if err != nil {
		logger.Fatal().AnErr("NewLogExporter", err).Msg("main")
	}
	if err := e.Start(); err != nil {
		logger.Fatal().AnErr("e.Start()", err).Msg("main")
	}
	defer e.Stop()
	defer e.Close()

	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})

	frontendKey, err = tag.NewKey("example.com/keys/frontend")
	if err != nil {
		logger.Fatal().AnErr("tag.NewKey", err).Msg("main")
	}
	videoSize = stats.Int64("example.com/measure/video_size", "size of processed videos", stats.UnitBytes)
	view.SetReportingPeriod(2 * time.Second)

	// Create view to see the processed video size
	// distribution broken down by frontend.
	// Register will allow view data to be exported.
	if err := view.Register(&view.View{
		Name:        "example.com/views/video_size",
		Description: "processed video size over time",
		TagKeys:     []tag.Key{frontendKey},
		Measure:     videoSize,
		Aggregation: view.Distribution(1<<16, 1<<32),
	}); err != nil {
		logger.Fatal().AnErr("view.Register", err).Msg("main")
	}

	// Process the video.
	if err := process(ctx); err != nil {
		logger.Fatal().AnErr("process", err).Msg("main")
	}

	// Wait for a duration longer than reporting duration to ensure the stats
	// library reports the collected data.
	fmt.Println("Wait longer than the reporting duration...")
	time.Sleep(4 * time.Second)
}

// process processes the video and instruments the processing
// by creating a span and collecting metrics about the operation.
func process(ctx context.Context) error {
	ctx, err := tag.New(ctx,
		tag.Insert(frontendKey, "mobile-ios9.3.5"),
	)
	if err != nil {
		return errors.Wrap(err, "tag.New")
	}
	ctx, span := trace.StartSpan(ctx, "example.com/ProcessVideo")
	defer span.End()
	// Process video.
	// Record the processed video size.

	// Sleep for [1,10] milliseconds to fake work.
	time.Sleep(time.Duration(rand.Intn(10)+1) * time.Millisecond)

	stats.Record(ctx, videoSize.M(25648))

	return nil
}
