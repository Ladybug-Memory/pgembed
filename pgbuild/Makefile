SHELL := /bin/bash
INSTALL_PREFIX := $(shell pwd)/../src/pgserver/pginstall/
BUILD := $(shell pwd)/pgbuild/

.PHONY: all
all: pgvector postgres

### postgres
POSTGRES_VERSION := 18-stable
POSTGRES_SRC := REL_18_STABLE
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
		&& export PG_CONFIG=$(INSTALL_PREFIX)/bin/pg_config \
		&& $(MAKE) -C $(PGVECTOR_DIR) -j \
		&& $(MAKE) -C $(PGVECTOR_DIR) install

.PHONY: pgvector
pgvector: postgres $(INSTALL_PREFIX)/lib/vector.so

### other
.PHONY: clean clean-all
clean:
	rm -rf $(INSTALL_PREFIX)
	rm -rf $(POSTGRES_SRC)
	rm -rf $(POSTGRES_BLD)
	rm -rf $(PGVECTOR_DIR)

clean-all: clean
	rm -rf *.tar.gz
