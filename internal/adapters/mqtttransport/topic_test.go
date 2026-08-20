package mqtttransport

import "testing"

func TestEventTopicAndEdgeSubscription(t *testing.T) {
	template := "telemetry/{edge_id}/{sensor_id}"
	topic, err := eventTopic(template, "edge-m0-0", "87575")
	if err != nil {
		t.Fatal(err)
	}
	if topic != "telemetry/edge-m0-0/87575" {
		t.Fatalf("topic = %q", topic)
	}
	subscription, err := edgeSubscription(template, "edge-m0-0")
	if err != nil {
		t.Fatal(err)
	}
	if subscription != "telemetry/edge-m0-0/+" {
		t.Fatalf("subscription = %q", subscription)
	}
}
