CARGO ?= cargo
BIN := rstn
PREFIX ?= $(HOME)/.local
INSTALL_DIR ?= $(PREFIX)/bin

.PHONY: build release test install install-local uninstall check

build:
	$(CARGO) build -p rustern

release:
	$(CARGO) build --release -p rustern

test:
	$(CARGO) test --workspace

check: release test

install: release
	$(CARGO) install --path . --locked --force
	@echo "Installed: $$(command -v $(BIN) 2>/dev/null || echo '$$HOME/.cargo/bin/$(BIN)')"
	@echo "Ensure cargo bin is on PATH: export PATH=\"$$HOME/.cargo/bin:$$PATH\""

install-local: release
	install -d "$(INSTALL_DIR)"
	install -m 755 "target/release/$(BIN)" "$(INSTALL_DIR)/$(BIN)"
	@echo "Installed: $(INSTALL_DIR)/$(BIN)"

uninstall:
	-$(CARGO) uninstall rustern
	-rm -f "$(INSTALL_DIR)/$(BIN)"

