package mqtttopic

const TelemetrySubscription = "sensors/+/telemetry"

func Telemetry(sensorID string) string {
	return "sensors/" + sensorID + "/telemetry"
}

func ReplayEnd(edgeID string) string {
	return "replay/" + edgeID + "/end"
}
