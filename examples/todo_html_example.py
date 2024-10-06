import sys
sys.path.append("src")
import rsql
from fasttag import *
from rsql.html import *
db = rsql.Database("dbs/todo_html_example.db")

todo = db.table("todo", id=rsql.AUTOINCREMENT, task=str, done=bool)
todo.print()
app, rtx = rsql_html_app()
register_tables(rtx, db)

# http://localhost:5001
@rtx('/')
def get():
    incomplete_count = todo.where(done=False).count()
    return Div(
        H1("TODO6"),
        A("ulli", href="/ulli"),
        Form(
            Input(name="task", placeholder="Task",id="task"),
            Button("Add"),
            onsubmit=add
        ),
        show_if(incomplete_count, Button("Complete all", onclick=todo.update_urlm({}, done=True))),
        show_if(incomplete_count.map(lambda x: not x), Button("Complete none", onclick=todo.update_urlm({}, done=False))),
        table(todo,
            lambda row:(Td(row.task),
                        Td(Input(type="checkbox", checked=row.done, onchange=row.update_urlm(done=not row.done))),
                        Td(Button("x", onclick=lambda: row.delete()))),
            header=(Th("Task"), Th("Done"), Th("Delete")), id="todos"),
        show_if(todo.where(done=True).count(), Button("Clear completed", onclick=todo.delete_urlm(done=True))),
        Span(value(incomplete_count.map(lambda x: "1 item left" if x == 1 else f"{x} items left"))),
    )

@rtx('/ulli')
def get():
    return Div(
        H1("TODO"),
        A("back", href="/"),
        Form(
            Input(name="task", placeholder="Task"),
            Button("Add"),
            hx_post="/add"
        ),
        ulli(todo,
            lambda row: (
                Span(row["task"]),
                Input(type="checkbox", checked=row.done,
                      onchange=row.update_urlm(done=not row.done)),
                Button("x", onclick=row.delete_urlm())
            ),
            header="Tasks", id="todos"),
    )

@rtx('/add')
def post(task:str):
    todo.insert(task=task, done=False)
    return Input(id="task", name="task", placeholder="Task", hx_swap_oob="outerHTML")

def add(task:str):
    todo.insert(task=task, done=False)
    return Input(id="task", name="task", placeholder="Task", hx_swap_oob="outerHTML")

@rtx('/hello')
def get():
    incomplete_count = todo.where(done=False).count()
    l=lambda row:(Td(row.task),
                Td(Input(type="checkbox", checked=row.done, onchange=row.update_urlm(done=not row.done))),
                Td(Button("x", onclick=lambda: row.delete())))

    a = Div(
            H1("TODO"),
            A("ulli", href="/ulli"),
            Form(
                Input(name="task", placeholder="Task",id="task"),
                Button("Add"),
                onsubmit=add
            ),
            show_if(incomplete_count, Button("Complete all", onclick=todo.update_urlm({}, done=True))),
            show_if(incomplete_count.map(lambda x: not x), Button("Complete none", onclick=todo.update_urlm({}, done=False))),
            table(todo,
                l,
                header=(Th("Task"), Th("Done"), Th("Delete")), id="todos"),
            show_if(todo.where(done=True).count(), Button("Clear completed", onclick=todo.delete_urlm(done=True))),
            Span(value(incomplete_count.map(lambda x: "1 item left" if x == 1 else f"{x} items left"))),
    )
    return "hello"

if __name__ == '__main__':
    import threading
    import time
    reqs = 0
    def print_memory():
        secs = 0
        global reqs
        while True:
            time.sleep(1)
            secs += 1
            import psutil
            process = psutil.Process()
            memory_info = process.memory_info()
            print(f"Used memory: {memory_info.rss / 1024 / 1024:.2f} MB, reqs: {reqs}, secs: {secs}, rps: {reqs/secs}")

    # print_memory_thread = threading.Thread(target=print_memory, daemon=True)
    # print_memory_thread.start()
    # # Start a thread to repeatedly call the server
    def call_server():
        global reqs
        import socket
        import time
        time.sleep(0.1)
        while True:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(('localhost', 5001))
                s.sendall(b'GET /hello HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n')
                response = s.recv(1024)
                s.close()
                reqs += 1
            except Exception as e:
                print(f"Error calling server: {e}")
                raise e

    server_call_thread = threading.Thread(target=call_server, daemon=True)
    # server_call_thread.start()
    # serve()
    rsql_html_serve()
