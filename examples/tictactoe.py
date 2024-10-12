import sys
sys.path.append("src")
from fasttag import *
from rsql.html import *

db = rsql.Database("dbs/tictactoe.db")
steps = db.table("steps", step=int, col=int, row=int, o=bool)
app, rtx = rsql_html_app(db=db)

def col(c, v):
    return Td(value(steps.where(col=c, row=v).only().map(
        lambda x: ("X" if x['step'] % 2 else "O") if x else
                   Button("", hx_post=f"/add?col={c}&row={v}"))))

won_table = (steps.group_by("col", "o", count="COUNT(*)").where(count=3).union_all(
    steps.group_by("row", "o", count="COUNT(*)").where(count=3))).union_all(
    steps.select(rpc="row+col", o=True).group_by("rpc", "o", count="COUNT(*)").where(count=3)).union_all(
    steps.select(rmc="row-col", o=True).group_by("rmc", "o", count="COUNT(*)").where(count=3))

won_count = won_table.count()
step_count = steps.count()

# http://localhost:5001
@rtx('/')
def get(auth):
    return Div(
        H1("Tic Tac Toe", id="title"),
        "Steps: ",
        value(step_count),
        "won: ", value(won_count.map(lambda x: "Yes" if x else "No")),
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
    steps.insert(step=step_count.value + 1, col=col, row=row, o=(step_count.value % 2))

if __name__ == '__main__':
    rsql_html_serve()