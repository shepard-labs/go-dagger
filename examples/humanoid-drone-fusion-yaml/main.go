package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/shepard-labs/go-dagger/pkg/dag"
	"github.com/shepard-labs/go-dagger/pkg/orchestrator"
	"github.com/shepard-labs/go-dagger/pkg/task"
)

type RunState struct {
	MissionID            string   `json:"mission_id,omitempty"`
	Area                 string   `json:"area,omitempty"`
	HumanoidID           string   `json:"humanoid_id,omitempty"`
	DroneID              string   `json:"drone_id,omitempty"`
	SafetyPolicy         string   `json:"safety_policy,omitempty"`
	HumanoidTelemetry    []string `json:"humanoid_telemetry,omitempty"`
	DroneTelemetry       []string `json:"drone_telemetry,omitempty"`
	EnvironmentSensors   []string `json:"environment_sensors,omitempty"`
	ReferenceFrame       string   `json:"reference_frame,omitempty"`
	WorldModel           string   `json:"world_model,omitempty"`
	Detections           []string `json:"detections,omitempty"`
	TerrainAssessment    []string `json:"terrain_assessment,omitempty"`
	SystemHealth         []string `json:"system_health,omitempty"`
	HumanoidActions      []string `json:"humanoid_actions,omitempty"`
	DroneActions         []string `json:"drone_actions,omitempty"`
	OperatorBriefing     []string `json:"operator_briefing,omitempty"`
	SafetyGateStatus     string   `json:"safety_gate_status,omitempty"`
	CoordinatedTimeline  []string `json:"coordinated_timeline,omitempty"`
	PublishedPlanURI     string   `json:"published_plan_uri,omitempty"`
	EstimatedMissionTime string   `json:"estimated_mission_time,omitempty"`
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

	orch, err := orchestrator.NewOrchestrator[RunState](ctx, orchestrator.Config{PostgresDSN: dsn, GlobalTimeout: 3 * time.Minute})
	if err != nil {
		return err
	}
	defer func() { _ = orch.Close() }()

	fmt.Println("loaded YAML DAG", d.Name, "with concurrency limit", d.ConcurrencyLimit)
	run, err := orch.Run(ctx, d, orchestrator.GlobalInputs[RunState]{
		Value: RunState{
			MissionID:    "fusion_warehouse_survey_042",
			Area:         "north distribution yard",
			HumanoidID:   "humanoid-atlas-07",
			DroneID:      "drone-scout-12",
			SafetyPolicy: "human-in-the-loop, geofenced, non-contact inspection",
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
		"examples.fusion.initialize_mission":                initializeMission,
		"examples.fusion.ingest_humanoid_telemetry":         ingestHumanoidTelemetry,
		"examples.fusion.ingest_drone_telemetry":            ingestDroneTelemetry,
		"examples.fusion.ingest_environment_sensors":        ingestEnvironmentSensors,
		"examples.fusion.calibrate_reference_frames":        calibrateReferenceFrames,
		"examples.fusion.build_fused_world_model":           buildFusedWorldModel,
		"examples.fusion.detect_objects_and_people":         detectObjectsAndPeople,
		"examples.fusion.assess_terrain_and_airspace":       assessTerrainAndAirspace,
		"examples.fusion.estimate_system_health":            estimateSystemHealth,
		"examples.fusion.plan_humanoid_ground_actions":      planHumanoidGroundActions,
		"examples.fusion.plan_drone_aerial_actions":         planDroneAerialActions,
		"examples.fusion.plan_operator_briefing":            planOperatorBriefing,
		"examples.fusion.run_safety_gates":                  runSafetyGates,
		"examples.fusion.coordinate_cross_platform_actions": coordinateCrossPlatformActions,
		"examples.fusion.publish_fusion_mission_plan":       publishFusionMissionPlan,
	}
}

func initializeMission(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "loading mission envelope and platform identities")
	if err := sleep(ctx, 250*time.Millisecond); err != nil {
		return state, err
	}

	logStep(ctx, "mission=%s humanoid=%s drone=%s", state.MissionID, state.HumanoidID, state.DroneID)
	return state, nil
}

func ingestHumanoidTelemetry(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "reading joint state, foot contact, battery, and local pose")
	if err := sleep(ctx, 750*time.Millisecond); err != nil {
		return state, err
	}
	state.HumanoidTelemetry = []string{"battery=82%", "joint-health=nominal", "pose=yard-a3", "payload=inspection-kit"}
	logStep(ctx, "humanoid telemetry=%s", strings.Join(state.HumanoidTelemetry, ", "))
	return state, nil
}

func ingestDroneTelemetry(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "reading flight state, GNSS, battery, and gimbal status")
	if err := sleep(ctx, 650*time.Millisecond); err != nil {
		return state, err
	}
	state.DroneTelemetry = []string{"battery=76%", "gnss=rtk-fixed", "altitude=18m", "camera=online"}
	logStep(ctx, "drone telemetry=%s", strings.Join(state.DroneTelemetry, ", "))
	return state, nil
}

func ingestEnvironmentSensors(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "reading lidar, depth, thermal, audio, and wind observations")
	if err := sleep(ctx, 900*time.Millisecond); err != nil {
		return state, err
	}
	state.EnvironmentSensors = []string{"lidar=active", "thermal=active", "wind=4m/s", "visibility=clear"}
	logStep(ctx, "environment sensors=%s", strings.Join(state.EnvironmentSensors, ", "))
	return state, nil
}

func calibrateReferenceFrames(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "aligning humanoid, drone, and site-map reference frames")
	if err := sleep(ctx, 400*time.Millisecond); err != nil {
		return state, err
	}
	state.ReferenceFrame = "site-map-v3:yard-north"
	logStep(ctx, "reference frame=%s", state.ReferenceFrame)
	return state, nil
}

func buildFusedWorldModel(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "building shared scene graph from platform and environment observations")
	if err := sleep(ctx, 550*time.Millisecond); err != nil {
		return state, err
	}
	state.WorldModel = fmt.Sprintf("world-model://%s/%s", state.Area, state.MissionID)
	logStep(ctx, "world model=%s", state.WorldModel)
	return state, nil
}

func detectObjectsAndPeople(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "detecting people, vehicles, tools, pallets, and blocked routes")
	if err := sleep(ctx, 1000*time.Millisecond); err != nil {
		return state, err
	}
	state.Detections = []string{"2 people near loading bay", "forklift in lane b", "blocked pallet stack at a3", "inspection marker m-17"}
	logStep(ctx, "detections=%s", strings.Join(state.Detections, "; "))
	return state, nil
}

func assessTerrainAndAirspace(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "checking walkable terrain, clearance, no-fly zones, and wind margin")
	if err := sleep(ctx, 850*time.Millisecond); err != nil {
		return state, err
	}
	state.TerrainAssessment = []string{"ground-route=a1-a3-clear", "stairs=avoid", "airspace=clear-below-25m", "wind-margin=good"}
	logStep(ctx, "terrain and airspace=%s", strings.Join(state.TerrainAssessment, ", "))
	return state, nil
}

func estimateSystemHealth(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "estimating energy reserve, thermal state, link quality, and actuator health")
	if err := sleep(ctx, 700*time.Millisecond); err != nil {
		return state, err
	}
	state.SystemHealth = []string{"humanoid-energy=31min", "drone-energy=24min", "network=stable", "thermal=nominal"}
	logStep(ctx, "system health=%s", strings.Join(state.SystemHealth, ", "))
	return state, nil
}

func planHumanoidGroundActions(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "planning humanoid navigation, inspection, manipulation, and standby tasks")
	if err := sleep(ctx, 650*time.Millisecond); err != nil {
		return state, err
	}
	state.HumanoidActions = []string{"walk to inspection marker m-17", "scan pallet stack from 2m", "standby outside forklift lane"}
	logStep(ctx, "humanoid plan=%s", strings.Join(state.HumanoidActions, " -> "))
	return state, nil
}

func planDroneAerialActions(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "planning drone orbit, mapping, relay, and overwatch tasks")
	if err := sleep(ctx, 600*time.Millisecond); err != nil {
		return state, err
	}
	state.DroneActions = []string{"orbit loading bay at 18m", "map lane b", "maintain visual overwatch", "relay comms if link drops"}
	logStep(ctx, "drone plan=%s", strings.Join(state.DroneActions, " -> "))
	return state, nil
}

func planOperatorBriefing(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "preparing operator summary, confidence, constraints, and questions")
	if err := sleep(ctx, 450*time.Millisecond); err != nil {
		return state, err
	}
	state.OperatorBriefing = []string{"confidence=0.91", "constraint=avoid forklift lane", "question=confirm pallet stack inspection priority"}
	logStep(ctx, "operator briefing=%s", strings.Join(state.OperatorBriefing, ", "))
	return state, nil
}

func runSafetyGates(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "checking geofence, collision, human proximity, and energy reserve gates")
	if err := sleep(ctx, 500*time.Millisecond); err != nil {
		return state, err
	}
	state.SafetyGateStatus = "passed"
	logStep(ctx, "safety gates=%s", state.SafetyGateStatus)
	return state, nil
}

func coordinateCrossPlatformActions(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "building conflict-free humanoid, drone, and operator timeline")
	if err := sleep(ctx, 500*time.Millisecond); err != nil {
		return state, err
	}
	state.CoordinatedTimeline = []string{
		"t+00 drone maps lane b",
		"t+20 humanoid walks to marker m-17",
		"t+45 drone overwatches loading bay",
		"t+60 operator confirms inspection priority",
		"t+90 humanoid completes non-contact inspection",
	}
	state.EstimatedMissionTime = "2m15s"
	logStep(ctx, "timeline=%s", strings.Join(state.CoordinatedTimeline, " | "))
	return state, nil
}

func publishFusionMissionPlan(ctx context.Context, state *RunState) (*RunState, error) {
	logStep(ctx, "publishing coordinated fusion mission plan")
	if err := sleep(ctx, 300*time.Millisecond); err != nil {
		return state, err
	}
	state.PublishedPlanURI = fmt.Sprintf("s3://fusion-plans/%s/plan.json", state.MissionID)
	logStep(ctx, "published plan=%s estimated_mission_time=%s", state.PublishedPlanURI, state.EstimatedMissionTime)
	return state, nil
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
