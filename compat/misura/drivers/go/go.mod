module compatprova

go 1.24

require go.mongodb.org/mongo-driver/v2 v2.4.0

require (
	github.com/golang/snappy v1.0.0 // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	golang.org/x/crypto v0.33.0 // indirect
	golang.org/x/sync v0.11.0 // indirect
	golang.org/x/text v0.22.0 // indirect
)

replace go.mongodb.org/mongo-driver/v2 => /tmp/claude-0/godrv

replace golang.org/x/crypto => github.com/golang/crypto v0.33.0

replace golang.org/x/sync => github.com/golang/sync v0.11.0

replace golang.org/x/text => github.com/golang/text v0.22.0

replace golang.org/x/tools => github.com/golang/tools v0.21.1-0.20240508182429-e35e4ccd0d2d

replace golang.org/x/mod => github.com/golang/mod v0.17.0

replace golang.org/x/net => github.com/golang/net v0.25.0

replace golang.org/x/sys => github.com/golang/sys v0.30.0

replace golang.org/x/term => github.com/golang/term v0.29.0

replace golang.org/x/telemetry => github.com/golang/telemetry v0.0.0-20240228155512-f48c80bd79b2
