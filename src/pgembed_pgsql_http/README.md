# pgembed-http

`pgsql-http` ([pramsey/pgsql-http](https://github.com/pramsey/pgsql-http))
as a separate `pgembed` extension package.

Install alongside the base package (which stays HTTP-free):

```bash
pip install pgembed pgembed-http
```

```python
import pgembed

assert pgembed.has_extension("pgsql_http")

server = pgembed.get_server("/path/to/data")
server.create_extension("http")   # CREATE EXTENSION http
```

Available functions depend on the bundled pgsql-http version
(`SELECT * FROM pg_available_extensions WHERE name = 'http'` after install).
Typical entry points: `http_get`, `http_post`, `http_header`, `http_response`,
`http_response_code`.

Requires libcurl dev headers at *build* time only
(`libcurl4-openssl-dev` on Debian/Ubuntu); the wheel ships a prebuilt
`http.so`, one file per OS/arch.
