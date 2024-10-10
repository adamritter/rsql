import sys, time
sys.path.append("src")
import rsql
import fasthtml
from fasttag import *
from rsql.html import *
from dotenv import load_dotenv
import os
import rsql.auth

# Load environment variables from the .env file
load_dotenv()

db = rsql.Database("dbs/todo_html_example.db")


todo = db.table("todo", id=rsql.AUTOINCREMENT, task=str, done=bool)
timet = db.table("time", id=rsql.AUTOINCREMENT, time=float)
timet.delete()
timet.insert(time=time.time())
counts = db.table("counts", id=rsql.AUTOINCREMENT, name=str, count=int)
# create a new thread that updates the time every second
import threading
# def update_time():
#     while True:
#         timet.update({}, time=time.time())
        # time.sleep(1)
# threading.Thread(target=update_time).start()
todo.print()
app, rtx = rsql_html_app(before=rsql.auth.before())

register_tables(rtx, db)
rsql.auth.setup(app)

counts.print()

# http://localhost:5001
@rtx('/')
def get(auth):
    incomplete_count = todo.where(done=False).count()
    return Div(
        table(counts, lambda row: (
            Tr(Td(row.name), Td(row.count))
        ), header=(Th("Name"), Th("Count")), id="counts"),
        H1("TODO", id="title"),
        Span("auth", auth), Br(),
        A("ulli", href="/ulli"),
        "Time: ",
        value(timet.only().map(lambda x: f"{x['time']}")),
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
        A("logout", href="/logout"),
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

if __name__ == '__main__':
    rsql_html_serve()