package mqtttransport

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"time"

	"continuum/internal/config"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

func connect(
	ctx context.Context,
	cfg config.MQTTConfig,
	clientID string,
	onConnectionUp func(*autopaho.ConnectionManager, *paho.Connack),
	onPublishReceived func(paho.PublishReceived) (bool, error),
) (*autopaho.ConnectionManager, error) {
	brokerURL, err := url.Parse(cfg.BrokerURL)
	if err != nil {
		return nil, fmt.Errorf("parse MQTT broker URL: %w", err)
	}

	clientConfig := paho.ClientConfig{
		ClientID: clientID,
		OnClientError: func(err error) {
			log.Printf("component=mqtt client_id=%s level=error error=%q", clientID, err)
		},
		OnServerDisconnect: func(disconnect *paho.Disconnect) {
			log.Printf(
				"component=mqtt client_id=%s level=warning server_disconnect_reason=%d",
				clientID,
				disconnect.ReasonCode,
			)
		},
	}
	if onPublishReceived != nil {
		clientConfig.OnPublishReceived = []func(paho.PublishReceived) (bool, error){
			onPublishReceived,
		}
	}

	connection, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{brokerURL},
		KeepAlive:                     uint16(cfg.KeepAlive.Duration / time.Second),
		CleanStartOnInitialConnection: true,
		SessionExpiryInterval:         0,
		ReconnectBackoff:              autopaho.NewConstantBackoff(time.Second),
		OnConnectionUp:                onConnectionUp,
		OnConnectError: func(err error) {
			log.Printf("component=mqtt client_id=%s level=warning connect_error=%q", clientID, err)
		},
		ClientConfig: clientConfig,
	})
	if err != nil {
		return nil, fmt.Errorf("create MQTT client %q: %w", clientID, err)
	}

	connectContext, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout.Duration)
	defer cancel()
	if err := connection.AwaitConnection(connectContext); err != nil {
		return nil, fmt.Errorf("connect MQTT client %q to %s: %w", clientID, cfg.BrokerURL, err)
	}
	return connection, nil
}

func disconnect(connection *autopaho.ConnectionManager) error {
	if connection == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := connection.Disconnect(ctx); err != nil {
		return fmt.Errorf("disconnect MQTT client: %w", err)
	}
	return nil
}
