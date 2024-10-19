from rsql.html import *

db = rsql.Database("dbs/tictactoe_history2.db")
steps = db.table("steps", step=int, col=int, row=int, o=bool)
nextstep = db.table("nextstep", nextstep=int)
nextstep.delete()
nextstep.insert(nextstep=1)
app, rtx = rsql_html_app(db=db)
past_steps = steps.join(nextstep).where("nextstep > step")
past_steps.print()

@memoize
def col(c, v):
    return past_steps.where(col=c, row=v).only().map(
        lambda x: ("X" if x['step'] % 2 else "O") if x else
                   Button("", hx_post=f"/add?col={c}&row={v}"))

won_table = (past_steps.group_by("col", "o", count="COUNT(*)").where(count=3).union_all(
    past_steps.group_by("row", "o", count="COUNT(*)").where(count=3))).union_all(
    past_steps.select(rpc="row+col", o=True).group_by("rpc", "o", count="COUNT(*)").where(count=3)).union_all(
    past_steps.select(rmc="row-col", o=True).group_by("rmc", "o", count="COUNT(*)").where(count=3))

won_count = won_table.count()
step_count = steps.count()
nextstep_only = nextstep.only()

css = """
body, table, td, th {
    background-color: #fff;
    color: #000;
}
table.board {
    margin-top: 0.5em;
    margin-bottom: 0.5em;
    border-collapse: collapse;

}
table.board td button {
    background-color: #fff0;
    border: 0px;
    width: 100%;
    height: 100%;
}
table.board td {
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

def HBox(*children):
    return Table(Tr(*[Td(c) for c in children]))

# http://localhost:5001
@rtx('/')
def get():
    nextstep.print()
    return show_unless(won_count, "Next player: ", nextstep_only.map(lambda x: "O" if x.nextstep % 2 else "X")),HBox(
        (
        show_if(won_count, "won: ",  step_count.map(lambda x: "X" if x % 2 else "O")),
        Table(Tr(Td(col(1, 1)), Td(col(2, 1)), Td(col(3, 1))),
              Tr(Td(col(1, 2)), Td(col(2, 2)), Td(col(3, 2))),
              Tr(Td(col(1, 3)), Td(col(2, 3)), Td(col(3, 3))), _class="board")),
              Span(
        
        table(steps, lambda x: (Td(Button("Go to move #" + str(x['step']), onclick=nextstep.update_urlm({}, nextstep=x['step']+1)))),
              header=(Td(Button("Go to start", onclick=nextstep.update_urlm({}, nextstep=1))))
              )
    )), Style(css), nextstep, Br(), table(past_steps, lambda x: (Td(x['step']), Td(x['col']), Td(x['row']), Td(x['o'])), header=(Td("Step"), Td("Col"), Td("Row"), Td("Player")))

@rtx('/reset')
def post():
    steps.delete()
    nextstep.update({}, nextstep=1)

@rtx('/add')
def post(col:int, row:int):
    if won_count.value == 0:
        # steps.delete("step >= ?", nextstep_only.value.nextstep)
        db.execute("delete from steps where step >= ?", (nextstep_only.value.nextstep,))
        steps.insert(step=nextstep_only.value.nextstep, col=col, row=row, o=(nextstep_only.value.nextstep % 2))
        nextstep.update({}, nextstep=nextstep_only.value.nextstep + 1)
if __name__ == '__main__':
    serve()