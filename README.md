# rsql
 Reactive SQL library + HTMX/FastHTML based server side Python web framework

 The framework automates all updates to the DOM based on changes to the database.

 Most of the syntax for the UI is finalized, but there is a lot of work to be done on
 production readiness, documentation, and examples.

 ## Production Readiness features missing
 - Memory leaks (get rid of all memory when HTTP/2.0 closes / timeout)
 - Authentication (look at SimpleHTML)
 - Authorization (ACLs and attribute based access control)
 - Serving (look at both SimpleHTML and how Rails does it in docker)
 - More complex exmample app (multi user chat / todo list)
 - PIP, dependency management
 - Pypy support, improve performance
 - More examples, better documentation, better performance

## Clone the repository:
```
git clone https://github.com/adamritter/rsql.git
```

Install Python 3.12

## Install dependencies:
```
pip install python-fasthtml
```

## Run examples:
```
python examples/qt_todo.py
```

## Run tests
```
python tests/test.py
```

Features:
- Async multithreading support