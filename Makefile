# llm-bot Makefile
# 常用命令：
#   make build     编译到 bin/bot
#   make run       使用 configs/config.yaml 启动
#   make tidy      同步 go.mod
#   make vet       go vet 静态检查
#   make test      跑单元测试（当前项目还没有测试用例）
#   make clean     清理编译产物

BIN       := bin/bot
PKG       := ./cmd/bot
CONFIG    := configs/config.yaml
GOFLAGS   := -trimpath

.PHONY: build run tidy vet test clean

build:
	@mkdir -p bin
	go build $(GOFLAGS) -o $(BIN) $(PKG)

run: build
	./$(BIN) -config $(CONFIG)

tidy:
	go mod tidy

vet:
	go vet ./...

test:
	go test ./...

clean:
	rm -rf bin
