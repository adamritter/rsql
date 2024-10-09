# rsql
 Reactive SQL library + HTMX/FastHTML based server side Python web framework

 The framework automates all updates to the DOM based on changes to the database.

 Most of the syntax for the UI is finalized, but there is a lot of work to be done on
 production readiness, documentation, and examples.

 ## Production Readiness features missing
- Extra websocket or http keep-alive connection to send async oob data
 - Authentication (look at SimpleHTML)
 - Authorization (ACLs and attribute based access control)
 - Serving on remote server (look at both SimpleHTML and how Rails does it in docker)
 - More complex example app (multi user chat / todo list)
 - PIP, dependency management
 - Pypy support, improve performance (or at least don't crash)
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