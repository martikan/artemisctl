APP_NAME=artemisctl
BUILD_PATH=bin

.PHONY: clean
clean:
	rm -rf $(BUILD_PATH)/
	go mod tidy

.PHONY: build
build: clean
	go build -o $(BUILD_PATH)/$(APP_NAME) main.go

run: build
	$(BUILD_PATH)/$(APP_NAME)

.PHONY: release
release: clean release-x86 release-arm

release-x86:
	CGO_ENABLED=0 go build -ldflags "-s -w" -o $(BUILD_PATH)/$(APP_NAME)-x86 main.go

release-arm:
	CGO_ENABLED=0 GOARCH=arm64 GOOS=darwin go build -ldflags "-s -w" -o $(BUILD_PATH)/$(APP_NAME)-arm main.go