# llm-bot Makefile
# 常用命令：
#   make build     编译到 bin/bot
#   make run       使用 configs/config.yaml 启动
#   make restart   拉取代码并重启 bot
#   make tidy      同步 go.mod
#   make vet       go vet 静态检查
#   make test      跑单元测试（当前项目还没有测试用例）
#   make clean     清理编译产物

BIN       := bin/bot
PKG       := ./cmd/bot
CONFIG    := configs/config.yaml
GOFLAGS   := -trimpath

.PHONY: build run restart tidy vet test clean

build:
	@mkdir -p bin
	go build $(GOFLAGS) -o $(BIN) $(PKG)

run: build
	nohup ./$(BIN) -config $(CONFIG) > output.log 2>&1 &

restart:
	git pull
	@echo "当前 bot 进程："
	@ps -ef | grep '[b]in/bot' || true
	@PIDS=$$(pgrep -f '(^|/|[.]/)$(BIN)[[:space:]].*-config[[:space:]]$(CONFIG)' || true); \
	if [ -n "$$PIDS" ]; then \
		echo "kill $$PIDS"; \
		kill $$PIDS; \
	fi
	$(MAKE) run

tidy:
	go mod tidy

vet:
	go vet ./...

test:
	go test ./...

clean:
	rm -rf bin
