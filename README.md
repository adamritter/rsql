# rsql
 Reactive SQL library + HTMX/FastHTML based server side Python web framework

 The framework automates all updates to the DOM based on changes to the database.

 Most of the syntax for the UI is finalized, but there is a lot of work to be done on
 production readiness, documentation, and examples.

 ## Production Readiness features missing
 - Multithreading support (right now the server has a huge bug when 2 requests are sent at the same time)
 - Memory leaks (get rid of all memory when HTTP/2.0 closes / timeout)
 - Async support (use context vars instead of globals)
 - Authentication (look at SimpleHTML)
 - Authorization (ACLs and attribute based access control)
- Serving (look at both SimpleHTML and how Rails does it in docker)
- More complex exmample app (multi user chat / todo list)
- PIP, dependency management

## Run tests
```
python tests/test.py
```
