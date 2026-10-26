package main

import (
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// The version of protocol/openapi.yaml this harness speaks, and the oldest
// whose clients it still serves.
const (
	protocolVersion        = 1
	minimumProtocolVersion = 1
)

type protocolView struct {
	Version int `json:"version"`
	Minimum int `json:"minimum"`
}

// sessionView is what a client opens the app with.
type sessionView struct {
	Protocol        protocolView          `json:"protocol"`
	User            sessionUser           `json:"user"`
	RateLimit       api.RateLimit         `json:"rateLimit"`
	Models          []domain.SessionModel `json:"models"`
	Auto            autoView              `json:"auto"`
	DefaultModel    string                `json:"defaultModel"`
	Widgets         []string              `json:"widgets"`
	ProfileDefaults domain.Profile        `json:"profileDefaults"`
	Composer        attach.ComposerView   `json:"composer"`
	Presets         []domain.Preset       `json:"presets"`
	Features        map[string]bool       `json:"features"`
}

type sessionUser struct {
	ID        string `json:"id"`
	Anonymous bool   `json:"anonymous"`
}

// autoView is how hard Auto works: its default level, and each level's
// labels.
type autoView struct {
	Default string                     `json:"default"`
	Levels  []domain.IntelligenceLevel `json:"levels"`
}
