from rsql.html import *
db = Database("dbs/infinite_scroll.db")
square = db.table("square2", id="INTEGER PRIMARY KEY", y=int)
app, rtx = rsql_html_app(pico=True, db=db)
if square.count().value < 1000:
    square.delete()
    for x in range(1000):
        square.insert(id=x, y=x*x)

@rtx('/')
def get():
    return Div(H1("Infinite Scroll"), table(square, infinite=True))

if __name__ == "__main__":
    serve()
