from rsql.html import *
db = Database("dbs/infinite_scroll.db")
square = db.table("square2", id="INTEGER PRIMARY KEY", y=int)
app, rtx = rsql_html_app(pico=True, db=db)
if square.count().value < 1000:
    square.delete()
    for x in range(1000):
        square.insert(id=x, y=x*x)

t = square.sort(limit=50)
@rtx('/')
def get():
    return Div(H1("Infinite Scroll"),
               table(t),
               Button("Next", hx_post="/next")), t.set_limit(100)

@rtx('/next')
def post():
    print("len(t.reset_cbs)", len(t.reset_cbs))
    t.set_limit(t.limit+50)

if __name__ == "__main__":
    serve()
