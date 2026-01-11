.DEFAULT_GOAL := build
.PHONY: build wheel install-wheel install-dev clean test

build:
	@if command -v yum > /dev/null 2>&1; then \
		yum install -y flex; \
	fi
	$(MAKE) -d -C pgbuild all

wheel: build
	python setup.py bdist_wheel

install-wheel: wheel
	python -m pip install --force-reinstall dist/*.whl

install-dev: build
	python -m pip install --force-reinstall -e .

clean:
	rm -rf build/ wheelhouse/ dist/ .eggs/
	$(MAKE) -C pgbuild clean

test:
	true
