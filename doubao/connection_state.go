package doubao

import (
	"fmt"
	"sync"
	"time"
)

// ConnectionState represents the current state of the WebSocket connection
type ConnectionState int

const (
	// StateDisconnected means no connection is established
	StateDisconnected ConnectionState = iota
	// StateConnecting means a connection attempt is in progress
	StateConnecting
	// StateConnected means the connection is established and ready
	StateConnected
	// StateReconnecting means attempting to reconnect after a failure
	StateReconnecting
	// StateFailed means all reconnection attempts have been exhausted
	StateFailed
)

// String returns a string representation of the connection state
func (s ConnectionState) String() string {
	switch s {
	case StateDisconnected:
		return "Disconnected"
	case StateConnecting:
		return "Connecting"
	case StateConnected:
		return "Connected"
	case StateReconnecting:
		return "Reconnecting"
	case StateFailed:
		return "Failed"
	default:
		return fmt.Sprintf("Unknown(%d)", s)
	}
}

// ReconnectConfig holds configuration for reconnection behavior
type ReconnectConfig struct {
	// Enabled controls whether automatic reconnection is enabled
	Enabled bool
	// MaxAttempts is the maximum number of reconnection attempts (0 = unlimited)
	MaxAttempts int
	// InitialDelay is the delay before the first reconnection attempt
	InitialDelay time.Duration
	// MaxDelay is the maximum delay between reconnection attempts
	MaxDelay time.Duration
	// BackoffMultiplier is multiplied by the current delay after each failed attempt
	BackoffMultiplier float64
}

// DefaultReconnectConfig returns a sensible default reconnection configuration
func DefaultReconnectConfig() *ReconnectConfig {
	return &ReconnectConfig{
		Enabled:           true,
		MaxAttempts:       10,               // Try up to 10 times
		InitialDelay:      1 * time.Second,  // Start with 1 second
		MaxDelay:          60 * time.Second, // Cap at 60 seconds
		BackoffMultiplier: 2.0,              // Double delay each time
	}
}

// connectionStateManager manages connection state transitions and reconnection logic
type connectionStateManager struct {
	mu                sync.RWMutex
	state             ConnectionState
	reconnectConfig   *ReconnectConfig
	currentAttempt    int
	currentDelay      time.Duration
	onStateChange     func(oldState, newState ConnectionState)
	onReconnectFailed func(attempts int, err error)
}

// newConnectionStateManager creates a new connection state manager
func newConnectionStateManager(config *ReconnectConfig) *connectionStateManager {
	if config == nil {
		config = DefaultReconnectConfig()
	}
	return &connectionStateManager{
		state:           StateDisconnected,
		reconnectConfig: config,
		currentDelay:    config.InitialDelay,
	}
}

// GetState returns the current connection state (thread-safe)
func (m *connectionStateManager) GetState() ConnectionState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

// SetState updates the connection state and triggers callbacks (thread-safe)
func (m *connectionStateManager) SetState(newState ConnectionState) {
	m.mu.Lock()
	oldState := m.state
	m.state = newState

	// Reset reconnection state when successfully connected
	if newState == StateConnected {
		m.currentAttempt = 0
		m.currentDelay = m.reconnectConfig.InitialDelay
	}

	callback := m.onStateChange
	m.mu.Unlock()

	// Execute callback outside the lock to avoid deadlocks
	if callback != nil && oldState != newState {
		callback(oldState, newState)
	}
}

// ShouldReconnect determines if reconnection should be attempted
func (m *connectionStateManager) ShouldReconnect() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.reconnectConfig.Enabled {
		return false
	}

	// If MaxAttempts is 0, allow unlimited attempts
	if m.reconnectConfig.MaxAttempts == 0 {
		return true
	}

	return m.currentAttempt < m.reconnectConfig.MaxAttempts
}

// IncrementAttempt increments the reconnection attempt counter
func (m *connectionStateManager) IncrementAttempt() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentAttempt++
}

// GetCurrentAttempt returns the current reconnection attempt number
func (m *connectionStateManager) GetCurrentAttempt() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentAttempt
}

// GetNextDelay calculates and returns the next reconnection delay with exponential backoff
func (m *connectionStateManager) GetNextDelay() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()

	delay := m.currentDelay

	// Calculate next delay with exponential backoff
	nextDelay := time.Duration(float64(m.currentDelay) * m.reconnectConfig.BackoffMultiplier)

	// Cap at maximum delay
	if nextDelay > m.reconnectConfig.MaxDelay {
		nextDelay = m.reconnectConfig.MaxDelay
	}

	m.currentDelay = nextDelay
	return delay
}

// Reset resets the reconnection state
func (m *connectionStateManager) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentAttempt = 0
	m.currentDelay = m.reconnectConfig.InitialDelay
}

// SetOnStateChange sets the callback for state changes
func (m *connectionStateManager) SetOnStateChange(callback func(oldState, newState ConnectionState)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onStateChange = callback
}

// SetOnReconnectFailed sets the callback for when all reconnection attempts fail
func (m *connectionStateManager) SetOnReconnectFailed(callback func(attempts int, err error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onReconnectFailed = callback
}

// NotifyReconnectFailed triggers the reconnection failed callback
func (m *connectionStateManager) NotifyReconnectFailed(err error) {
	m.mu.RLock()
	callback := m.onReconnectFailed
	attempts := m.currentAttempt
	m.mu.RUnlock()

	if callback != nil {
		callback(attempts, err)
	}
}
