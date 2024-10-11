import sys, time
sys.path.append("src")
import rsql
from fasttag import *
from rsql.html import *
from dotenv import load_dotenv
import rsql.auth

# Load environment variables from the .env file
load_dotenv()

db = rsql.Database("dbs/tictactoe.db")
steps = db.table("steps", step=int, col=int, row=int)
app, rtx = rsql_html_app()

register_tables(rtx, db)

def col(c, v):
    return Td(value(steps.where(col=c, row=v).only().map(lambda x: ("X" if x['step'] % 2 else "O") if x else
                                                Button("", hx_post=f"/add?col={c}&row={v}"))))

# http://localhost:5001
@rtx('/')
def get(auth):
    return Div(
        H1("Tic Tac Toe", id="title"),
        "Steps: ",
        value(steps.count()),
        Table(Tr(col(1, 1), col(2, 1), col(3, 1)),
              Tr(col(1, 2), col(2, 2), col(3, 2)),
              Tr(col(1, 3), col(2, 3), col(3, 3))),
        table(steps.sort("step"),
              lambda x: (Td(x['col']), Td(x['row']), Td(x['step'])),
              header=(Th("Col"), Th("Row"), Th("Step"))),
        Button("Reset", onclick=steps.delete_urlm()),
    )

@rtx('/add')
def post(col:int, row:int):
    steps.insert(step=steps.count().value + 1, col=col, row=row)

if __name__ == '__main__':
    rsql_html_serve()