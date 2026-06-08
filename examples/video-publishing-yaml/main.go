package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/shepard-labs/go-dagger/pkg/dag"
	"github.com/shepard-labs/go-dagger/pkg/orchestrator"
	"github.com/shepard-labs/go-dagger/pkg/task"
)

type RunState struct {
	VideoID         string   `json:"video_id,omitempty"`
	Title           string   `json:"title,omitempty"`
	SourceURI       string   `json:"source_uri,omitempty"`
	DurationSeconds int      `json:"duration_seconds,omitempty"`
	Codec           string   `json:"codec,omitempty"`
	Renditions      []string `json:"renditions,omitempty"`
	ThumbnailURI    string   `json:"thumbnail_uri,omitempty"`
	CaptionsURI     string   `json:"captions_uri,omitempty"`
	SafetyStatus    string   `json:"safety_status,omitempty"`
	PackageManifest string   `json:"package_manifest,omitempty"`
	PublishedURL    string   `json:"published_url,omitempty"`
	WorkflowStarted string   `json:"workflow_started,omitempty"`
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	_ = godotenv.Load(".env")
	_ = godotenv.Load("../../.env")
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		return fmt.Errorf("POSTGRES_DSN is required")
	}

	data, err := os.ReadFile(examplePath("dag.yaml"))
	if err != nil {
		return err
	}
	d, err := dag.ParseYAML(data, functions(), nil, nil)
	if err != nil {
		return err
	}

	orch, err := orchestrator.NewOrchestrator[RunState](ctx, orchestrator.Config{PostgresDSN: dsn, GlobalTimeout: 2 * time.Minute})
	if err != nil {
		return err
	}
	defer func() { _ = orch.Close() }()

	fmt.Println("loaded YAML DAG", d.Name, "with concurrency limit", d.ConcurrencyLimit)
	run, err := orch.Run(ctx, d, orchestrator.GlobalInputs[RunState]{
		Value: RunState{
			VideoID:   "vid_2026_0606_launch_trailer",
			Title:     "Launch Trailer: Summer Collection",
			SourceURI: "s3://media-ingest/raw/vid_2026_0606_launch_trailer/master.mov",
		},
	})
	if err != nil {
		return err
	}
	fmt.Println("run", run.ID, "finished for", d.Name)
	return nil
}

func functions() task.FunctionRegistry[RunState] {
	return task.FunctionRegistry[RunState]{
		"examples.video.ingest_upload":               ingestUpload,
		"examples.video.probe_media":                 probeMedia,
		"examples.video.transcode_1080p":             transcode1080p,
		"examples.video.transcode_720p":              transcode720p,
		"examples.video.generate_thumbnail":          generateThumbnail,
		"examples.video.extract_captions":            extractCaptions,
		"examples.video.content_safety_scan":         contentSafetyScan,
		"examples.video.assemble_publishing_package": assemblePublishingPackage,
		"examples.video.publish_video":               publishVideo,
	}
}

func ingestUpload(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "accepting source upload")
	if err := sleep(ctx, 300*time.Millisecond); err != nil {
		return state, err
	}
	state.WorkflowStarted = time.Now().Format(time.RFC3339)
	logStep(ctx, "stored %s", state.SourceURI)
	return state, nil
}

func probeMedia(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "probing %s", state.SourceURI)
	if err := sleep(ctx, 450*time.Millisecond); err != nil {
		return state, err
	}
	state.DurationSeconds = 142
	state.Codec = "prores-422-hq"
	logStep(ctx, "duration=%ds codec=%s", state.DurationSeconds, state.Codec)
	return state, nil
}

func transcode1080p(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "started")
	if err := sleep(ctx, 1400*time.Millisecond); err != nil {
		return state, err
	}
	state.Renditions = append(state.Renditions, artifact(state.VideoID, "1080p.m3u8"))
	logStep(ctx, "created %s", artifact(state.VideoID, "1080p.m3u8"))
	return state, nil
}

func transcode720p(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "started")
	if err := sleep(ctx, 1100*time.Millisecond); err != nil {
		return state, err
	}
	state.Renditions = append(state.Renditions, artifact(state.VideoID, "720p.m3u8"))
	logStep(ctx, "created %s", artifact(state.VideoID, "720p.m3u8"))
	return state, nil
}

func generateThumbnail(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "started")
	if err := sleep(ctx, 650*time.Millisecond); err != nil {
		return state, err
	}
	state.ThumbnailURI = artifact(state.VideoID, "poster.jpg")
	logStep(ctx, "created %s", state.ThumbnailURI)
	return state, nil
}

func extractCaptions(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "started")
	if err := sleep(ctx, 800*time.Millisecond); err != nil {
		return state, err
	}
	state.CaptionsURI = artifact(state.VideoID, "captions.vtt")
	logStep(ctx, "created %s", state.CaptionsURI)
	return state, nil
}

func contentSafetyScan(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "started")
	if err := sleep(ctx, 900*time.Millisecond); err != nil {
		return state, err
	}
	state.SafetyStatus = "approved"
	logStep(ctx, "status=%s", state.SafetyStatus)
	return state, nil
}

func assemblePublishingPackage(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "joining generated artifacts")
	if err := sleep(ctx, 350*time.Millisecond); err != nil {
		return state, err
	}
	artifacts := []string{
		artifact(state.VideoID, "1080p.m3u8"),
		artifact(state.VideoID, "720p.m3u8"),
		artifact(state.VideoID, "poster.jpg"),
		artifact(state.VideoID, "captions.vtt"),
	}
	sort.Strings(artifacts)
	state.Renditions = []string{artifact(state.VideoID, "1080p.m3u8"), artifact(state.VideoID, "720p.m3u8")}
	state.ThumbnailURI = artifact(state.VideoID, "poster.jpg")
	state.CaptionsURI = artifact(state.VideoID, "captions.vtt")
	state.SafetyStatus = "approved"
	state.PackageManifest = artifact(state.VideoID, "manifest.json")
	logStep(ctx, "manifest=%s artifacts=%s", state.PackageManifest, strings.Join(artifacts, ", "))
	return state, nil
}

func publishVideo(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "publishing %s", state.PackageManifest)
	if err := sleep(ctx, 250*time.Millisecond); err != nil {
		return state, err
	}
	state.PublishedURL = fmt.Sprintf("https://video.example.com/watch/%s", state.VideoID)
	logStep(ctx, "published %q at %s", state.Title, state.PublishedURL)
	return state, nil
}

func artifact(videoID, name string) string {
	return fmt.Sprintf("s3://media-publish/%s/%s", videoID, name)
}

func sleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func logStep(ctx context.Context, format string, args ...any) {
	orchestrator.LoggerFromContext(ctx).Info(fmt.Sprintf(format, args...))
}

func examplePath(name string) string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return name
	}
	return filepath.Join(filepath.Dir(file), name)
}
