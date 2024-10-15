import sys
sys.path.append("src")
from rsql.html import *

db = rsql.Database("dbs/tictactoe_simple.db")
steps = db.table("steps", step=int, col=int, row=int, o=bool)
app, rtx = rsql_html_app(db=db)

@memoize
def col(c, v):
    return steps.where(col=c, row=v).only().map(
        lambda x: ("X" if x['step'] % 2 else "O") if x else
                   Button("", hx_post=f"/add?col={c}&row={v}"))

won_table = (steps.group_by("col", "o", count="COUNT(*)").where(count=3).union_all(
    steps.group_by("row", "o", count="COUNT(*)").where(count=3))).union_all(
    steps.select(rpc="row+col", o=True).group_by("rpc", "o", count="COUNT(*)").where(count=3)).union_all(
    steps.select(rmc="row-col", o=True).group_by("rmc", "o", count="COUNT(*)").where(count=3))

won_count = won_table.count()
step_count = steps.count()

css = """
body, table, td, th {
    background-color: #fff;
    color: #000;
}
table {
    margin-top: 0.5em;
    margin-bottom: 0.5em;
    border-collapse: collapse;

}
td button {
    background-color: #fff0;
    border: 0px;
    width: 100%;
    height: 100%;
}
td {
    border: 1px solid #000;
    padding: 0px;
    font-weight: bold;
    font-size: 1.5em;
    width: 1em;
    height: 1em;
    text-align: center;
    vertical-align: middle;
}
"""

# http://localhost:5001
@rtx('/')
def get():
    return Div(
        show_unless(won_count, "Next player: ", step_count.map(lambda x: "O" if x % 2 else "X")),
        show_if(won_count, "won: ",  step_count.map(lambda x: "X" if x % 2 else "O")),
        Table(Tr(Td(col(1, 1)), Td(col(2, 1)), Td(col(3, 1))),
              Tr(Td(col(1, 2)), Td(col(2, 2)), Td(col(3, 2))),
              Tr(Td(col(1, 3)), Td(col(2, 3)), Td(col(3, 3)))),
        Button("Reset", onclick=steps.delete_urlm()),
    ), Style(css)

@rtx('/add')
def post(col:int, row:int):
    if won_count.value == 0:
        steps.insert(step=step_count.value + 1, col=col, row=row, o=(step_count.value % 2))

if __name__ == '__main__':
    rsql_html_serve()