// Package codebuddy registers thinking configuration for CodeBuddy models.
// The upstream chat API is OpenAI-compatible and uses reasoning_effort.
package codebuddy

import (
	"github.com/router-for-me/CLIProxyAPI/v7/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/thinking/provider/openai"
)

func init() {
	thinking.RegisterProvider("codebuddy", openai.NewApplier())
}
