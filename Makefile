all: build

GO ?= go

commands = $(shell $(GO) list -f '{{.Name}}:{{.ImportPath}}' ./... | grep ^main: | sed s/^main://)

build:
	$(foreach cmd,$(commands),$(GO) build $(cmd);)

check:
	$(GO) build ./...
	$(GO) test -v -cover ./...

clean:
	$(GO) clean
	$(RM) $(basename $(commands))

.PHONY: all build check clean
