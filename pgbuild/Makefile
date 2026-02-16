SHELL := /bin/bash
INSTALL_PREFIX := $(shell pwd)/../src/pgembed/pginstall
BUILD := $(shell pwd)/pgbuild/
IS_MUSL ?= $(shell if command -v ldd >/dev/null 2>&1 && ldd --version 2>&1 | grep -qi musl; then echo 1; else echo 0; fi)

# Detect Windows and add .exe extension for executables
ifeq ($(OS),Windows_NT)
    EXE_EXT := .exe
else
    EXE_EXT :=
endif

ifeq ($(OS),Windows_NT)
	RUST_EXTENSIONS_SUPPORTED := 0
else ifeq ($(IS_MUSL),1)
	RUST_EXTENSIONS_SUPPORTED := 0
else
	RUST_EXTENSIONS_SUPPORTED := 1
endif

EXTENSIONS ?= pgvector pgvectorscale pgtextsearch
ifneq ($(OS),Windows_NT)
EXTENSIONS += pg_duckdb
endif

.PHONY: all
all: postgres $(EXTENSIONS)

.PHONY: pgvector pgvectorscale pgtextsearch pg_duckdb
pgvector: postgres $(INSTALL_PREFIX)/lib/vector.so
pgvectorscale: postgres $(INSTALL_PREFIX)/lib/vectorscale.so
pgtextsearch: postgres $(INSTALL_PREFIX)/lib/pg_textsearch.so
pg_duckdb: postgres $(INSTALL_PREFIX)/lib/pg_duckdb.so

### postgres
POSTGRES_VERSION := 17-stable
POSTGRES_SRC := REL_17_STABLE
POSTGRES_BLD := $(POSTGRES_SRC)


## extract
$(POSTGRES_SRC)/configure:
	git clone https://github.com/postgres/postgres -b $(POSTGRES_SRC) --single-branch $(POSTGRES_SRC)
	touch $(POSTGRES_SRC)/configure

## configure
$(POSTGRES_BLD)/config.status: $(POSTGRES_SRC)/configure
	mkdir -p $(POSTGRES_BLD)
	cd $(POSTGRES_BLD) && ../$(POSTGRES_SRC)/configure --prefix=$(INSTALL_PREFIX) --without-readline --without-icu

## build
# https://stackoverflow.com/questions/68379786/
# for explanation of unsetting make env variables prior to calling postgres' own make
$(POSTGRES_BLD)/src/bin/initdb/initdb: $(POSTGRES_BLD)/config.status
	unset MAKELEVEL && unset MAKEFLAGS && unset MFLAGS && $(MAKE) -C $(POSTGRES_BLD) -j

## install to INSTALL_PREFIX
$(INSTALL_PREFIX)/bin/postgres: $(POSTGRES_BLD)/config.status
	mkdir -p $(INSTALL_PREFIX)
	unset MAKELEVEL && unset MAKEFLAGS && unset MFLAGS && $(MAKE) -C $(POSTGRES_BLD) install

.PHONY: postgres
postgres: $(INSTALL_PREFIX)/bin/postgres

### pgvector
PGVECTOR_TAG := v0.8.1
PGVECTOR_URL := https://github.com/pgvector/pgvector/archive/refs/tags/$(PGVECTOR_TAG).tar.gz
PGVECTOR_DIR := pgvector-$(PGVECTOR_TAG)

$(PGVECTOR_DIR).tar.gz:
	curl -L -o $(PGVECTOR_DIR).tar.gz $(PGVECTOR_URL)

$(PGVECTOR_DIR)/Makefile: $(PGVECTOR_DIR).tar.gz
	# tar extract into pgvector-$(PGVECTOR_TAG)
	mkdir -p $(PGVECTOR_DIR)
	tar xzf $(PGVECTOR_DIR).tar.gz -C $(PGVECTOR_DIR) --strip-components=1
	touch $(PGVECTOR_DIR)/Makefile

$(INSTALL_PREFIX)/lib/vector.so: $(PGVECTOR_DIR)/Makefile $(INSTALL_PREFIX)/bin/postgres
	unset MAKELEVEL && unset MAKEFLAGS && unset MFLAGS \
		&& $(MAKE) -C $(PGVECTOR_DIR) PG_CONFIG=$(INSTALL_PREFIX)/bin/pg_config$(EXE_EXT) -j \
		&& $(MAKE) -C $(PGVECTOR_DIR) PG_CONFIG=$(INSTALL_PREFIX)/bin/pg_config$(EXE_EXT) install

### pg_duckdb
PG_DUCKDB_TAG := v1.1.1
PG_DUCKDB_REPO := https://github.com/duckdb/pg_duckdb
PG_DUCKDB_DIR := pg_duckdb-$(PG_DUCKDB_TAG)

$(PG_DUCKDB_DIR)/Makefile:
	git clone --branch $(PG_DUCKDB_TAG) --single-branch $(PG_DUCKDB_REPO) $(PG_DUCKDB_DIR)
	cd $(PG_DUCKDB_DIR) && git submodule update --init --recursive
	touch $(PG_DUCKDB_DIR)/Makefile

$(INSTALL_PREFIX)/lib/pg_duckdb.so: $(PG_DUCKDB_DIR)/Makefile $(INSTALL_PREFIX)/bin/postgres
ifneq ($(OS),Windows_NT)
	unset MAKELEVEL && unset MAKEFLAGS && unset MFLAGS \
		&& $(MAKE) -C $(PG_DUCKDB_DIR) PG_CONFIG=$(INSTALL_PREFIX)/bin/pg_config$(EXE_EXT) DUCKDB_BUILD=ReleaseStatic install
else
	$(error pg_duckdb build is not supported on Windows due to cargo-pgrx issues)
endif

### pgvectorscale
PGVECTORSCALE_TAG := 0.5.1
PGVECTORSCALE_URL := https://github.com/timescale/pgvectorscale/archive/refs/tags/$(PGVECTORSCALE_TAG).tar.gz
PGVECTORSCALE_DIR := pgvectorscale-$(PGVECTORSCALE_TAG)

$(PGVECTORSCALE_DIR).tar.gz:
	curl -L -o $(PGVECTORSCALE_DIR).tar.gz $(PGVECTORSCALE_URL)

$(PGVECTORSCALE_DIR)/pgvectorscale/Cargo.toml: $(PGVECTORSCALE_DIR).tar.gz
	mkdir -p $(PGVECTORSCALE_DIR)
	tar xzf $(PGVECTORSCALE_DIR).tar.gz -C $(PGVECTORSCALE_DIR) --strip-components=1
	touch $(PGVECTORSCALE_DIR)/pgvectorscale/Cargo.toml

$(INSTALL_PREFIX)/lib/vectorscale.so: $(PGVECTORSCALE_DIR)/pgvectorscale/Cargo.toml $(INSTALL_PREFIX)/bin/postgres
ifeq ($(RUST_EXTENSIONS_SUPPORTED),1)
	unset MAKELEVEL && unset MAKEFLAGS && unset MFLAGS \
		&& export PATH="$$HOME/.cargo/bin:$$PATH" \
		&& cd $(PGVECTORSCALE_DIR)/pgvectorscale \
		&& cargo pgrx init --pg17 $(INSTALL_PREFIX)/bin/pg_config$(EXE_EXT) \
		&& cargo pgrx install --release
else ifeq ($(OS),Windows_NT)
	$(error pgvectorscale build is not supported on Windows due to cargo-pgrx issues)
else
	$(error pgvectorscale build is not supported on musl libc distributions (e.g., Alpine) due to cargo-pgrx issues)
endif

### pgtextsearch
PGTEXTSEARCH_TAG := v0.5.0
PGTEXTSEARCH_URL := https://github.com/timescale/pg_textsearch/archive/refs/tags/$(PGTEXTSEARCH_TAG).tar.gz
PGTEXTSEARCH_DIR := pgtextsearch-$(PGTEXTSEARCH_TAG)

$(PGTEXTSEARCH_DIR).tar.gz:
	curl -L -o $(PGTEXTSEARCH_DIR).tar.gz $(PGTEXTSEARCH_URL)

$(PGTEXTSEARCH_DIR)/Makefile: $(PGTEXTSEARCH_DIR).tar.gz
	mkdir -p $(PGTEXTSEARCH_DIR)
	tar xzf $(PGTEXTSEARCH_DIR).tar.gz -C $(PGTEXTSEARCH_DIR) --strip-components=1
	touch $(PGTEXTSEARCH_DIR)/Makefile

$(INSTALL_PREFIX)/lib/pg_textsearch.so: $(PGTEXTSEARCH_DIR)/Makefile $(INSTALL_PREFIX)/bin/postgres
ifeq ($(RUST_EXTENSIONS_SUPPORTED),1)
	unset MAKELEVEL && unset MAKEFLAGS && unset MFLAGS \
		&& $(MAKE) -C $(PGTEXTSEARCH_DIR) PG_CONFIG=$(INSTALL_PREFIX)/bin/pg_config$(EXE_EXT) -j \
		&& $(MAKE) -C $(PGTEXTSEARCH_DIR) PG_CONFIG=$(INSTALL_PREFIX)/bin/pg_config$(EXE_EXT) install
else ifeq ($(OS),Windows_NT)
	$(error pgtextsearch build is not supported on Windows due to cargo-pgrx issues)
else
	$(error pgtextsearch build is not supported on musl libc distributions (e.g., Alpine) due to cargo-pgrx issues)
endif

### other
.PHONY: clean clean-all
clean:
	rm -rf $(INSTALL_PREFIX)
	rm -rf $(POSTGRES_SRC)
	rm -rf $(POSTGRES_BLD)
	rm -rf $(PGVECTOR_DIR)
	rm -rf $(PGVECTORSCALE_DIR)
	rm -rf $(PG_DUCKDB_DIR)
	rm -rf $(PGTEXTSEARCH_DIR)

clean-all: clean
	rm -rf *.tar.gz
