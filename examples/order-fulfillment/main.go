package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/shepard-labs/go-dagger/pkg/dag"
	"github.com/shepard-labs/go-dagger/pkg/orchestrator"
	"github.com/shepard-labs/go-dagger/pkg/task"
)

type RunState struct {
	OrderID           string            `json:"order_id,omitempty"`
	CustomerID        string            `json:"customer_id,omitempty"`
	CustomerEmail     string            `json:"customer_email,omitempty"`
	ShippingAddress   string            `json:"shipping_address,omitempty"`
	Items             []OrderItem       `json:"items,omitempty"`
	InventoryHolds    map[string]string `json:"inventory_holds,omitempty"`
	PaymentAuthorized bool              `json:"payment_authorized,omitempty"`
	PaymentID         string            `json:"payment_id,omitempty"`
	WarehouseID       string            `json:"warehouse_id,omitempty"`
	PickList          []string          `json:"pick_list,omitempty"`
	PackageID         string            `json:"package_id,omitempty"`
	Carrier           string            `json:"carrier,omitempty"`
	TrackingNumber    string            `json:"tracking_number,omitempty"`
	CustomerMessage   string            `json:"customer_message,omitempty"`
}

type OrderItem struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
	Bin      string `json:"bin,omitempty"`
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	d := &dag.DAG[RunState]{
		Name:             "order-fulfillment-example",
		ConcurrencyLimit: 2,
		TaskOrder:        []string{"receive-order", "reserve-inventory", "authorize-payment", "plan-fulfillment", "pack-shipment", "book-carrier", "notify-customer"},
		Tasks:            map[string]*task.Task[RunState]{},
	}

	d.Tasks["receive-order"] = &task.Task[RunState]{
		Name:         "receive-order",
		FunctionName: "examples.order.receive",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			fmt.Println("order received", state.OrderID, "for", len(state.Items), "items")
			return state, nil
		},
	}

	d.Tasks["reserve-inventory"] = &task.Task[RunState]{
		Name:         "reserve-inventory",
		DependsOn:    []string{"receive-order"},
		FunctionName: "examples.order.reserve_inventory",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			state.InventoryHolds = make(map[string]string, len(state.Items))
			for _, item := range state.Items {
				state.InventoryHolds[item.SKU] = fmt.Sprintf("hold-%s-%d", state.OrderID, item.Quantity)
			}
			fmt.Println("reserved inventory", strings.Join(mapKeys(state.InventoryHolds), ", "))
			return state, nil
		},
	}

	d.Tasks["authorize-payment"] = &task.Task[RunState]{
		Name:         "authorize-payment",
		DependsOn:    []string{"receive-order"},
		FunctionName: "examples.order.authorize_payment",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			units := 0
			for _, item := range state.Items {
				units += item.Quantity
			}
			state.PaymentAuthorized = true
			state.PaymentID = fmt.Sprintf("pay_%s_%d_units", state.OrderID, units)
			fmt.Println("authorized payment", state.PaymentID)
			return state, nil
		},
	}

	d.Tasks["plan-fulfillment"] = &task.Task[RunState]{
		Name:         "plan-fulfillment",
		DependsOn:    []string{"reserve-inventory", "authorize-payment"},
		FunctionName: "examples.order.plan_fulfillment",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			if !state.PaymentAuthorized {
				return state, fmt.Errorf("payment was not authorized for %s", state.OrderID)
			}
			state.WarehouseID = "sfo-warehouse-1"
			for _, item := range state.Items {
				state.PickList = append(state.PickList, fmt.Sprintf("%dx %s from bin %s", item.Quantity, item.SKU, item.Bin))
			}
			fmt.Println("planned fulfillment from", state.WarehouseID)
			return state, nil
		},
	}

	d.Tasks["pack-shipment"] = &task.Task[RunState]{
		Name:         "pack-shipment",
		DependsOn:    []string{"plan-fulfillment"},
		FunctionName: "examples.order.pack_shipment",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			state.PackageID = fmt.Sprintf("pkg_%s_%d_lines", state.OrderID, len(state.PickList))
			fmt.Println("packed shipment", state.PackageID, "with", strings.Join(state.PickList, "; "))
			return state, nil
		},
	}

	d.Tasks["book-carrier"] = &task.Task[RunState]{
		Name:         "book-carrier",
		DependsOn:    []string{"pack-shipment"},
		FunctionName: "examples.order.book_carrier",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			state.Carrier = "UPS Ground"
			state.TrackingNumber = strings.ToUpper(strings.ReplaceAll(state.PackageID, "pkg_", "1Z"))
			fmt.Println("booked carrier", state.Carrier, state.TrackingNumber)
			return state, nil
		},
	}

	d.Tasks["notify-customer"] = &task.Task[RunState]{
		Name:         "notify-customer",
		DependsOn:    []string{"book-carrier"},
		FunctionName: "examples.order.notify_customer",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			state.CustomerMessage = fmt.Sprintf("Order %s shipped via %s. Track it with %s.", state.OrderID, state.Carrier, state.TrackingNumber)
			fmt.Println("notified", state.CustomerEmail, state.CustomerMessage)
			return state, nil
		},
	}

	return runDAG(ctx, d)
}

func runDAG(ctx context.Context, d *dag.DAG[RunState]) error {
	if err := d.Validate(); err != nil {
		return err
	}
	_ = godotenv.Load(".env")
	_ = godotenv.Load("../../.env")
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		return fmt.Errorf("POSTGRES_DSN is required")
	}
	orch, err := orchestrator.NewOrchestrator[RunState](ctx, orchestrator.Config{PostgresDSN: dsn, GlobalTimeout: 2 * time.Minute})
	if err != nil {
		return err
	}
	defer func() { _ = orch.Close() }()
	run, err := orch.Run(ctx, d, orchestrator.GlobalInputs[RunState]{
		Value: RunState{
			OrderID:         "ord_1042",
			CustomerID:      "cus_77",
			CustomerEmail:   "alex@example.com",
			ShippingAddress: "120 Market St, San Francisco, CA",
			Items:           []OrderItem{{SKU: "coffee-beans", Quantity: 2, Bin: "A-14"}, {SKU: "ceramic-mug", Quantity: 1, Bin: "C-02"}},
		},
	})
	if err != nil {
		return err
	}
	fmt.Println("run", run.ID, "finished for", d.Name)
	return nil
}

func mapKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}
