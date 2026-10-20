// Package httpx carries the HTTP client identity the gateway presents to the
// upstream inference API.
package httpx

// userAgent is the User-Agent the official OpenCode CLI sends, in the
// opencode/<channel>/<version>/<name> shape produced by App.useragent()
// (packages/core/src/app.ts) with the CLI's own app metadata. The free-tier
// gate on opencode.ai/inference requires that shape with a version at or
// above 1.18.0: anything else is answered with 426 UpgradeRequired, and a UA
// that does not read as the CLI is rejected with 403 FreeTierError.
const userAgent = "opencode/latest/2.0.32/cli"

// UserAgent returns the upstream User-Agent header value.
func UserAgent() string {
	return userAgent
}
