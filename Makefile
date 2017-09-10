all: build

GO ?= go
GO_TEST_FLAGS ?= -v

commands = $(shell $(GO) list -f '{{.Name}}:{{.ImportPath}}' ./... | grep ^main: | sed s/^main://)

build:
	$(foreach cmd,$(commands),$(GO) build $(cmd);)

check:
	$(GO) build ./...
	$(GO) test $(GO_TEST_FLAGS) ./...

clean:
	$(GO) clean
	$(RM) $(basename $(commands))

.PHONY: all build check clean
