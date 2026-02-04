package main

import "embed"

//go:embed WEBUI/*
var webFS embed.FS
