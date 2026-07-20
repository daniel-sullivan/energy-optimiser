package actuator

import (
	"context"
	"fmt"
	"log/slog"

	"energy-optimiser/config"
	"energy-optimiser/ha"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Actuator executes optimizer decisions: MQTT for battery, HA for loads.
// In dry-run mode, all commands are logged but not executed.
type Actuator struct {
	mqtt    mqtt.Client
	ha      *ha.Client
	mqttCfg config.MQTT
	dryRun  bool
}

func New(mqttCfg config.MQTT, haClient *ha.Client, dryRun bool) (*Actuator, error) {
	a := &Actuator{
		ha:      haClient,
		mqttCfg: mqttCfg,
		dryRun:  dryRun,
	}

	if !dryRun {
		opts := mqtt.NewClientOptions().
			AddBroker(mqttCfg.Broker).
			SetClientID("energy-optimiser")
		if mqttCfg.Username != "" {
			opts.SetUsername(mqttCfg.Username)
			opts.SetPassword(mqttCfg.Password)
		}

		client := mqtt.NewClient(opts)
		if tok := client.Connect(); tok.Wait() && tok.Error() != nil {
			return nil, fmt.Errorf("mqtt connect: %w", tok.Error())
		}
		a.mqtt = client
	} else {
		slog.Info("actuator: dry-run mode — commands will be logged only")
	}

	return a, nil
}

func (a *Actuator) Close() {
	if a.mqtt != nil {
		a.mqtt.Disconnect(1000)
	}
}

// SetGridCharge enables or disables grid charging via the SRNE controller.
func (a *Actuator) SetGridCharge(on bool) error {
	topic := a.mqttCfg.CommandTopic("switch", "charge_from_mains")
	payload := "OFF"
	if on {
		payload = "ON"
	}

	if a.dryRun {
		slog.Info("DRY-RUN: would set grid charge", "on", on, "topic", topic, "payload", payload)
		return nil
	}

	slog.Info("actuator: grid charge", "on", on, "topic", topic)
	tok := a.mqtt.Publish(topic, 0, false, payload)
	tok.Wait()
	return tok.Error()
}

// SetChargerPriority sets the SRNE charger priority directly.
func (a *Actuator) SetChargerPriority(priority string) error {
	topic := a.mqttCfg.CommandTopic("select", "charger_priority")

	if a.dryRun {
		slog.Info("DRY-RUN: would set charger priority", "priority", priority, "topic", topic)
		return nil
	}

	slog.Info("actuator: charger priority", "priority", priority)
	tok := a.mqtt.Publish(topic, 0, false, priority)
	tok.Wait()
	return tok.Error()
}

// SetMaxChargeSOC sets the SOC threshold at which grid charging stops.
func (a *Actuator) SetMaxChargeSOC(pct int) error {
	topic := a.mqttCfg.CommandTopic("number", "stop_charge_soc")

	if a.dryRun {
		slog.Info("DRY-RUN: would set max charge SOC", "pct", pct, "topic", topic)
		return nil
	}

	slog.Info("actuator: max charge soc", "pct", pct)
	tok := a.mqtt.Publish(topic, 0, false, fmt.Sprintf("%d", pct))
	tok.Wait()
	return tok.Error()
}

// SetLoad turns a deferrable load on or off via Home Assistant.
func (a *Actuator) SetLoad(ctx context.Context, entityID string, on bool) error {
	service := "turn_off"
	if on {
		service = "turn_on"
	}

	if a.dryRun {
		slog.Info("DRY-RUN: would set load", "entity", entityID, "on", on, "service", service)
		return nil
	}

	slog.Info("actuator: load", "entity", entityID, "on", on)
	return a.ha.CallService(ctx, "switch", service, map[string]any{
		"entity_id": entityID,
	})
}
